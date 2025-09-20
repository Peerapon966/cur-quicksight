import * as cdk from 'aws-cdk-lib';
import * as glue from 'aws-cdk-lib/aws-glue';
import * as iam from 'aws-cdk-lib/aws-iam';
import * as s3 from 'aws-cdk-lib/aws-s3';

import { AppContext } from '../../lib/template/app-context';
import { StackConfig } from '../../lib/template/app-config';
import { BaseStack } from '../../lib/template/stack/base/base-stack';
import { createSharedParameters, SharedParameters } from '../../lib/cur-parameters';
import { assertNoHardCodedValues, deepClone, loadJsonTemplate } from '../../lib/util/json-template';

export interface GlueStackProps {
    readonly curExportBucket: s3.IBucket;
    readonly partnerListBucket: s3.IBucket;
}

export class GlueStack extends BaseStack {
    public readonly parameters: SharedParameters;
    public readonly database: glue.CfnDatabase;
    public readonly databaseName: string;

    constructor(appContext: AppContext, stackConfig: StackConfig, private readonly props: GlueStackProps) {
        super(appContext, stackConfig);

        this.parameters = createSharedParameters(this, {
            globalParameters: this.commonProps.appConfig?.Global?.Parameters,
            stackParameters: this.stackConfig?.Parameters,
        });

        this.databaseName = this.parameters.glueDatabaseName.value;
        this.database = new glue.CfnDatabase(this, 'CurDatabase', {
            catalogId: cdk.Aws.ACCOUNT_ID,
            databaseInput: {
                name: this.databaseName,
            },
        });

        const createCrawlersCondition = new cdk.CfnCondition(this, 'CreateGlueCrawlersCondition', {
            expression: cdk.Fn.conditionEquals(this.parameters.createGlueCrawlers.parameter.valueAsString, 'true'),
        });

        const createTablesCondition = new cdk.CfnCondition(this, 'CreateGlueTablesCondition', {
            expression: cdk.Fn.conditionEquals(this.parameters.createGlueCrawlers.parameter.valueAsString, 'false'),
        });

        this.createCrawlers(createCrawlersCondition);
        this.createTables(createTablesCondition);

        this.exportOutput('GlueDatabaseName', this.databaseName);
    }

    private createCrawlers(condition: cdk.CfnCondition) {
        const crawlerRole = new iam.Role(this, 'GlueCrawlerRole', {
            assumedBy: new iam.ServicePrincipal('glue.amazonaws.com'),
            managedPolicies: [iam.ManagedPolicy.fromAwsManagedPolicyName('service-role/AWSGlueServiceRole')],
        });

        crawlerRole.addToPolicy(new iam.PolicyStatement({
            effect: iam.Effect.ALLOW,
            actions: ['s3:GetObject', 's3:ListBucket'],
            resources: [
                this.props.curExportBucket.bucketArn,
                this.props.curExportBucket.arnForObjects('*'),
                this.props.partnerListBucket.bucketArn,
                this.props.partnerListBucket.arnForObjects('*'),
            ],
        }));

        const crawlerRoleResource = crawlerRole.node.defaultChild as iam.CfnRole;
        crawlerRoleResource.cfnOptions.condition = condition;

        const curCrawler = new glue.CfnCrawler(this, 'CurCrawler', {
            name: this.withProjectPrefix('cur-crawler'),
            databaseName: this.databaseName,
            role: crawlerRole.roleArn,
            schemaChangePolicy: {
                deleteBehavior: 'DEPRECATE_IN_DATABASE',
                updateBehavior: 'UPDATE_IN_DATABASE',
            },
            targets: {
                s3Targets: [
                    {
                        path: cdk.Fn.join('', ['s3://', this.props.curExportBucket.bucketName, '/']),
                    },
                ],
            },
        });
        curCrawler.cfnOptions.condition = condition;

        const partnerCrawler = new glue.CfnCrawler(this, 'PartnerCrawler', {
            name: this.withProjectPrefix('partner-crawler'),
            databaseName: this.databaseName,
            role: crawlerRole.roleArn,
            schemaChangePolicy: {
                deleteBehavior: 'DEPRECATE_IN_DATABASE',
                updateBehavior: 'UPDATE_IN_DATABASE',
            },
            targets: {
                s3Targets: [
                    {
                        path: cdk.Fn.join('', ['s3://', this.props.partnerListBucket.bucketName, '/IT info/']),
                    },
                    {
                        path: cdk.Fn.join('', ['s3://', this.props.partnerListBucket.bucketName, '/RLS/']),
                    },
                ],
            },
        });
        partnerCrawler.cfnOptions.condition = condition;
    }

    private createTables(condition: cdk.CfnCondition) {
        const curDefinition = this.prepareTableDefinition('assets/glue/cur_data.json', {
            name: this.parameters.curTableName.value,
            location: this.buildS3Uri(this.props.curExportBucket.bucketName, this.parameters.curTableName.value),
        });

        const itInfoDefinition = this.prepareTableDefinition('assets/glue/it_info.json', {
            name: 'it_info',
            location: this.buildS3Uri(this.props.partnerListBucket.bucketName, 'IT info/'),
        });

        const rlsDefinition = this.prepareTableDefinition('assets/glue/rls.json', {
            name: 'rls',
            location: this.buildS3Uri(this.props.partnerListBucket.bucketName, 'RLS/'),
        });

        const curTable = new glue.CfnTable(this, 'CurTable', {
            catalogId: cdk.Aws.ACCOUNT_ID,
            databaseName: this.databaseName,
            tableInput: curDefinition,
        });
        curTable.cfnOptions.condition = condition;

        const itInfoTable = new glue.CfnTable(this, 'ItInfoTable', {
            catalogId: cdk.Aws.ACCOUNT_ID,
            databaseName: this.databaseName,
            tableInput: itInfoDefinition,
        });
        itInfoTable.cfnOptions.condition = condition;

        const rlsTable = new glue.CfnTable(this, 'RlsTable', {
            catalogId: cdk.Aws.ACCOUNT_ID,
            databaseName: this.databaseName,
            tableInput: rlsDefinition,
        });
        rlsTable.cfnOptions.condition = condition;
    }

    private buildS3Uri(bucketName: string, suffix: string): string {
        if (cdk.Token.isUnresolved(suffix)) {
            return cdk.Fn.join('', ['s3://', bucketName, '/', suffix, '/']);
        }
        const normalized = suffix.endsWith('/') ? suffix : `${suffix}/`;
        return cdk.Fn.join('', ['s3://', bucketName, '/', normalized]);
    }

    private prepareTableDefinition(assetPath: string, overrides: { name: string; location: string }): glue.CfnTable.TableInputProperty {
        const definition = deepClone(loadJsonTemplate(assetPath));
        delete definition.DatabaseName;
        delete definition.CreateTime;
        delete definition.UpdateTime;
        delete definition.LastAccessTime;

        definition.Name = overrides.name;
        definition.StorageDescriptor.Location = overrides.location;
        definition.Parameters = definition.Parameters ?? {};
        delete definition.StorageDescriptor['SortColumns'];

        assertNoHardCodedValues(definition, assetPath);
        return definition;
    }
}
