import * as fs from 'fs';
import * as path from 'path';
import * as cdk from 'aws-cdk-lib';
import * as quicksight from 'aws-cdk-lib/aws-quicksight';

import { AppContext } from '../../lib/template/app-context';
import { StackConfig } from '../../lib/template/app-config';
import { BaseStack } from '../../lib/template/stack/base/base-stack';
import { createSharedParameters, ParameterDefinition, SharedParameters } from '../../lib/cur-parameters';
import { assertNoHardCodedValues, deepClone, loadJsonTemplate } from '../../lib/util/json-template';
import { AthenaStack } from './athena-stack';

export interface QuicksightStackProps {
    readonly athenaStack: AthenaStack;
}

interface InputColumnDefinition {
    readonly name: string;
    readonly type: string;
}

export class QuicksightStack extends BaseStack {
    public readonly parameters: SharedParameters;
    public readonly curDataSet: quicksight.CfnDataSet;
    public readonly rlsDataSet: quicksight.CfnDataSet;
    public readonly curDataSetId: string;
    public readonly rlsDataSetId: string;

    constructor(appContext: AppContext, stackConfig: StackConfig, private readonly props: QuicksightStackProps) {
        super(appContext, stackConfig);

        this.parameters = createSharedParameters(this, {
            globalParameters: this.commonProps.appConfig?.Global?.Parameters,
            stackParameters: this.stackConfig?.Parameters,
        });

        const curDataSource = this.createDataSource('Cur', this.props.athenaStack.curWorkGroupName);
        const rlsDataSource = this.createDataSource('Rls', this.props.athenaStack.rlsWorkGroupName);

        this.rlsDataSetId = this.toResourceId('rls-dataset');
        this.curDataSetId = this.toResourceId('cur-dataset');

        const rlsSql = this.renderSql('assets/quicksight/RLS-group-mapping.sql', this.parameters.glueDatabaseName.value, this.parameters.curTableName.value);
        const curSql = this.renderSql('assets/quicksight/CUR.sql', this.parameters.glueDatabaseName.value, this.parameters.curTableName.value);

        this.rlsDataSet = this.createRlsDataSet(rlsDataSource.attrArn, rlsSql);
        this.curDataSet = this.createCurDataSet(curDataSource.attrArn, curSql, this.rlsDataSet.attrArn);

        this.createRefreshSchedule('CurRefreshSchedule', this.curDataSetId, this.curDataSet, 'cur-daily-refresh');
        this.createRefreshSchedule('RlsRefreshSchedule', this.rlsDataSetId, this.rlsDataSet, 'rls-daily-refresh');

        this.createAnalysisAndDashboard();

        this.exportOutput('CurDataSetId', this.curDataSetId);
        this.exportOutput('CurDataSetArn', this.curDataSet.attrArn);
        this.exportOutput('RlsDataSetId', this.rlsDataSetId);
        this.exportOutput('RlsDataSetArn', this.rlsDataSet.attrArn);
    }

    private createDataSource(suffix: string, workGroupName: string): quicksight.CfnDataSource {
        const id = this.toResourceId(`${suffix.toLowerCase()}-athena-ds`);
        const dataSource = new quicksight.CfnDataSource(this, `${suffix}AthenaDataSource`, {
            awsAccountId: cdk.Aws.ACCOUNT_ID,
            dataSourceId: id,
            name: this.withProjectPrefix(`${suffix}-Athena`),
            type: 'ATHENA',
            dataSourceParameters: {
                athenaParameters: {
                    workGroup: workGroupName,
                },
            },
            permissions: [
                this.buildQuickSightPermission(this.parameters.quicksightAdminUserArn.value, [
                    'quicksight:DescribeDataSource',
                    'quicksight:DescribeDataSourcePermissions',
                    'quicksight:UpdateDataSourcePermissions',
                    'quicksight:UpdateDataSource',
                    'quicksight:DeleteDataSource',
                    'quicksight:PassDataSource',
                ]),
            ],
        });

        return dataSource;
    }

    private createCurDataSet(dataSourceArn: string, sql: string, rlsArn: string): quicksight.CfnDataSet {
        const physicalTableId = 'cur_sql';
        const logicalTableId = 'cur_logical';
        const columns: InputColumnDefinition[] = [
            { name: 'line_item_usage_account_name', type: 'STRING' },
            { name: 'line_item_usage_account_id', type: 'STRING' },
            { name: 'line_item_unblended_cost', type: 'DECIMAL' },
            { name: 'discount percent', type: 'INTEGER' },
            { name: 'discount usd', type: 'DECIMAL' },
            { name: 'partner price', type: 'DECIMAL' },
            { name: 'line_item_usage_start_date', type: 'DATETIME' },
            { name: 'line_item_product_code', type: 'STRING' },
            { name: 'line_item_line_item_type', type: 'STRING' },
            { name: 'partner customer code', type: 'STRING' },
            { name: 'partner name', type: 'STRING' },
        ];

        return new quicksight.CfnDataSet(this, 'CurDataSet', {
            awsAccountId: cdk.Aws.ACCOUNT_ID,
            dataSetId: this.curDataSetId,
            name: this.withProjectPrefix('CUR'),
            importMode: 'SPICE',
            physicalTableMap: {
                [physicalTableId]: {
                    customSql: {
                        columns: columns.map((column) => ({ name: column.name, type: column.type })),
                        dataSourceArn,
                        name: this.withProjectPrefix('CUR-SQL'),
                        sqlQuery: sql,
                    },
                },
            },
            logicalTableMap: {
                [logicalTableId]: {
                    alias: 'CUR',
                    source: {
                        physicalTableId,
                    },
                },
            },
            permissions: [
                this.buildQuickSightPermission(this.parameters.quicksightAdminUserArn.value, [
                    'quicksight:DescribeDataSet',
                    'quicksight:DescribeDataSetPermissions',
                    'quicksight:UpdateDataSetPermissions',
                    'quicksight:PassDataSet',
                    'quicksight:UpdateDataSet',
                    'quicksight:DeleteDataSet',
                    'quicksight:CreateIngestion',
                    'quicksight:CancelIngestion',
                    'quicksight:DescribeIngestion',
                    'quicksight:DescribeRefreshSchedule',
                    'quicksight:CreateRefreshSchedule',
                    'quicksight:DeleteRefreshSchedule',
                    'quicksight:UpdateRefreshSchedule',
                ]),
            ],
            rowLevelPermissionDataSet: {
                arn: rlsArn,
                permissionPolicy: 'GRANT_ACCESS',
            },
        });
    }

    private createRlsDataSet(dataSourceArn: string, sql: string): quicksight.CfnDataSet {
        const physicalTableId = 'rls_sql';
        const logicalTableId = 'rls_logical';
        const columns: InputColumnDefinition[] = [
            { name: 'Account ID', type: 'STRING' },
            { name: 'GroupName', type: 'STRING' },
        ];

        return new quicksight.CfnDataSet(this, 'RlsDataSet', {
            awsAccountId: cdk.Aws.ACCOUNT_ID,
            dataSetId: this.rlsDataSetId,
            name: this.withProjectPrefix('RLS-group-mapping'),
            importMode: 'SPICE',
            physicalTableMap: {
                [physicalTableId]: {
                    customSql: {
                        columns: columns.map((column) => ({ name: column.name, type: column.type })),
                        dataSourceArn,
                        name: this.withProjectPrefix('RLS-SQL'),
                        sqlQuery: sql,
                    },
                },
            },
            logicalTableMap: {
                [logicalTableId]: {
                    alias: 'RLS-group-mapping',
                    source: {
                        physicalTableId,
                    },
                },
            },
            permissions: [
                this.buildQuickSightPermission(this.parameters.quicksightAdminUserArn.value, [
                    'quicksight:DescribeDataSet',
                    'quicksight:DescribeDataSetPermissions',
                    'quicksight:UpdateDataSetPermissions',
                    'quicksight:PassDataSet',
                    'quicksight:UpdateDataSet',
                    'quicksight:DeleteDataSet',
                    'quicksight:CreateIngestion',
                    'quicksight:CancelIngestion',
                    'quicksight:DescribeIngestion',
                    'quicksight:DescribeRefreshSchedule',
                    'quicksight:CreateRefreshSchedule',
                    'quicksight:DeleteRefreshSchedule',
                    'quicksight:UpdateRefreshSchedule',
                ]),
            ],
        });
    }

    private createRefreshSchedule(id: string, dataSetId: string, dataset: quicksight.CfnDataSet, scheduleSuffix: string) {
        const scheduleTiming = this.resolveRefreshScheduleTiming();

        const schedule = new quicksight.CfnRefreshSchedule(this, id, {
            awsAccountId: cdk.Aws.ACCOUNT_ID,
            dataSetId,
            schedule: {
                scheduleId: this.toResourceId(scheduleSuffix),
                refreshType: 'FULL_REFRESH',
                scheduleFrequency: {
                    interval: 'DAILY',
                    timeOfTheDay: scheduleTiming.timeOfTheDay,
                    timeZone: scheduleTiming.timeZone,
                },
            },
        });
        schedule.node.addDependency(dataset);
    }

    private createAnalysisAndDashboard() {
        const datasetArn = this.curDataSet.attrArn;
        const datasetIdentifier = 'cur-dataset';

        const analysisDefinition = this.normalizeDefinitionKeys(
            deepClone(loadJsonTemplate('assets/quicksight/analysis_definition.json'))
        );
        this.applyDatasetReference(analysisDefinition, datasetArn, datasetIdentifier);
        assertNoHardCodedValues(analysisDefinition, 'analysis_definition.json');

        const analysis = new quicksight.CfnAnalysis(this, 'CurAnalysis', {
            awsAccountId: cdk.Aws.ACCOUNT_ID,
            analysisId: this.toResourceId('cur-analysis'),
            name: this.withProjectPrefix('CUR-analysis'),
            definition: analysisDefinition,
            permissions: [
                this.buildQuickSightPermission(this.parameters.quicksightAdminUserArn.value, [
                    'quicksight:DescribeAnalysis',
                    'quicksight:DescribeAnalysisPermissions',
                    'quicksight:UpdateAnalysisPermissions',
                    'quicksight:UpdateAnalysis',
                    'quicksight:DeleteAnalysis',
                    'quicksight:QueryAnalysis',
                ]),
            ],
        });
        analysis.node.addDependency(this.curDataSet);

        const dashboardDefinition = this.normalizeDefinitionKeys(
            deepClone(loadJsonTemplate('assets/quicksight/dashboard_definition.json'))
        );
        this.applyDatasetReference(dashboardDefinition, datasetArn, datasetIdentifier);
        assertNoHardCodedValues(dashboardDefinition, 'dashboard_definition.json');

        const dashboard = new quicksight.CfnDashboard(this, 'CurDashboard', {
            awsAccountId: cdk.Aws.ACCOUNT_ID,
            dashboardId: this.toResourceId('cur-dashboard'),
            name: this.withProjectPrefix('CUR-dashboard'),
            definition: dashboardDefinition,
            permissions: [
                this.buildQuickSightPermission(this.parameters.quicksightAdminUserArn.value, [
                    'quicksight:DescribeDashboard',
                    'quicksight:DescribeDashboardPermissions',
                    'quicksight:UpdateDashboard',
                    'quicksight:DeleteDashboard',
                    'quicksight:QueryDashboard',
                    'quicksight:ListDashboardVersions',
                    'quicksight:UpdateDashboardPermissions',
                ]),
            ],
        });
        dashboard.node.addDependency(analysis);
    }

    private applyDatasetReference(definition: any, datasetArn: string, identifier: string) {
        const declarationKey = 'dataSetIdentifierDeclarations';
        if (Array.isArray(definition[declarationKey])) {
            definition[declarationKey] = definition[declarationKey].map(() => ({
                identifier,
                dataSetArn: datasetArn,
            }));
        }

        this.replaceStringRecursively(definition, 'synnex-org-usage', identifier);
        this.replaceStringRecursively(
            definition,
            'arn:aws:quicksight:us-east-1:168000259484:dataset/f135f8e9-8ff0-4224-b130-e007b88ed65e',
            datasetArn
        );
    }

    private renderSql(filePath: string, databaseName: string, curTableName: string): string {
        const sql = fs.readFileSync(path.resolve(filePath), 'utf8');
        const replacedDb = sql.replace(/"cur-quicksight"/g, `"${databaseName}"`);
        return replacedDb.replace(/\.data/g, `.${curTableName}`);
    }

    private buildQuickSightPermission(principal: string, actions: string[]): quicksight.CfnDataSet.ResourcePermissionProperty {
        return {
            principal,
            actions,
        };
    }

    private toResourceId(suffix: string): string {
        const base = this.withProjectPrefix(suffix);
        return base.replace(/[^A-Za-z0-9_-]/g, '-').toLowerCase();
    }

    private resolveRefreshScheduleTiming(): { timeOfTheDay: string; timeZone: string } {
        const localTime = this.tryResolveParameter(this.parameters.dailyRefreshTimeLocal);
        const timezone = this.tryResolveParameter(this.parameters.timezone);

        if (localTime && timezone) {
            const utc = this.convertLocalTimeToUtc(localTime, timezone);
            return { timeOfTheDay: utc, timeZone: 'UTC' };
        }

        return {
            timeOfTheDay: this.parameters.dailyRefreshTimeLocal.value,
            timeZone: this.parameters.timezone.value,
        };
    }

    private tryResolveParameter(parameter: ParameterDefinition): string | undefined {
        if (parameter.resolved && parameter.resolved.length > 0) {
            return parameter.resolved;
        }

        if (!cdk.Token.isUnresolved(parameter.value) && parameter.value.length > 0) {
            return parameter.value;
        }

        return undefined;
    }

    private convertLocalTimeToUtc(localTime: string, timezone: string): string {
        const [hour, minute] = localTime.split(':').map((value) => parseInt(value, 10));
        const reference = new Date(Date.UTC(1970, 0, 1, hour, minute, 0, 0));
        const offset = this.getTimeZoneOffset(reference, timezone);
        const utcDate = new Date(reference.getTime() - offset);
        return `${utcDate.getUTCHours().toString().padStart(2, '0')}:${utcDate.getUTCMinutes().toString().padStart(2, '0')}`;
    }

    private getTimeZoneOffset(date: Date, timeZone: string): number {
        const dtf = new Intl.DateTimeFormat('en-US', {
            timeZone,
            year: 'numeric',
            month: '2-digit',
            day: '2-digit',
            hour: '2-digit',
            minute: '2-digit',
            second: '2-digit',
            hour12: false,
        });
        const parts = dtf.formatToParts(date);
        const data: Record<string, string> = {};
        for (const part of parts) {
            if (part.type !== 'literal') {
                data[part.type] = part.value;
            }
        }
        const asUtc = Date.UTC(
            parseInt(data.year, 10),
            parseInt(data.month, 10) - 1,
            parseInt(data.day, 10),
            parseInt(data.hour, 10),
            parseInt(data.minute, 10),
            parseInt(data.second, 10)
        );
        return asUtc - date.getTime();
    }

    private replaceStringRecursively(target: any, search: string, replace: string) {
        if (Array.isArray(target)) {
            target.forEach((item, index) => {
                if (typeof item === 'string' && item === search) {
                    target[index] = replace;
                } else {
                    this.replaceStringRecursively(item, search, replace);
                }
            });
            return;
        }

        if (target && typeof target === 'object') {
            Object.keys(target).forEach((key) => {
                const value = target[key];
                if (typeof value === 'string' && value === search) {
                    target[key] = replace;
                } else {
                    this.replaceStringRecursively(value, search, replace);
                }
            });
        }
    }

    private normalizeDefinitionKeys(value: any): any {
        if (Array.isArray(value)) {
            return value.map((item) => this.normalizeDefinitionKeys(item));
        }

        if (value && typeof value === 'object') {
            const normalized: Record<string, any> = {};
            Object.entries(value).forEach(([key, val]) => {
                const normalizedKey = key.length > 0 ? key[0].toLowerCase() + key.slice(1) : key;
                normalized[normalizedKey] = this.normalizeDefinitionKeys(val);
            });
            return normalized;
        }

        return value;
    }
}
