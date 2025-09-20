import * as cdk from 'aws-cdk-lib';
import * as athena from 'aws-cdk-lib/aws-athena';

import { AppContext } from '../../lib/template/app-context';
import { StackConfig } from '../../lib/template/app-config';
import { BaseStack } from '../../lib/template/stack/base/base-stack';
import { createSharedParameters, SharedParameters } from '../../lib/cur-parameters';

export interface AthenaStackProps {
}

export class AthenaStack extends BaseStack {
    public readonly parameters: SharedParameters;
    public readonly curWorkGroup: athena.CfnWorkGroup;
    public readonly rlsWorkGroup: athena.CfnWorkGroup;
    public readonly curWorkGroupName: string;
    public readonly rlsWorkGroupName: string;

    constructor(appContext: AppContext, stackConfig: StackConfig, _props?: AthenaStackProps) {
        super(appContext, stackConfig);

        this.parameters = createSharedParameters(this, {
            globalParameters: this.commonProps.appConfig?.Global?.Parameters,
            stackParameters: this.stackConfig?.Parameters,
        });

        this.curWorkGroupName = 'CUR-QuickSight';
        this.rlsWorkGroupName = 'CUR-QuickSight-RLS';

        this.curWorkGroup = new athena.CfnWorkGroup(this, 'CurQuickSightWorkGroup', {
            name: this.curWorkGroupName,
            state: 'ENABLED',
            workGroupConfiguration: {
                enforceWorkGroupConfiguration: true,
                engineVersion: { selectedEngineVersion: 'Athena engine version 3' },
                publishCloudWatchMetricsEnabled: true,
            },
        });

        const rlsOutputLocation = this.buildResultsLocation(
            this.parameters.rlsAthenaResultsBucketName.value,
            this.parameters.rlsAthenaResultsPrefix.value
        );

        this.rlsWorkGroup = new athena.CfnWorkGroup(this, 'CurQuickSightRlsWorkGroup', {
            name: this.rlsWorkGroupName,
            state: 'ENABLED',
            workGroupConfiguration: {
                enforceWorkGroupConfiguration: true,
                engineVersion: { selectedEngineVersion: 'Athena engine version 3' },
                publishCloudWatchMetricsEnabled: true,
                resultConfiguration: {
                    outputLocation: rlsOutputLocation,
                },
            },
        });

        this.exportOutput('CurAthenaWorkGroupName', this.curWorkGroupName);
        this.exportOutput('RlsAthenaWorkGroupName', this.rlsWorkGroupName);
    }

    public workGroupArn(name: string): string {
        return cdk.Stack.of(this).formatArn({
            service: 'athena',
            resource: 'workgroup',
            resourceName: name,
        });
    }

    private buildResultsLocation(bucketName: string, prefix: string): string {
        if (cdk.Token.isUnresolved(prefix)) {
            return cdk.Fn.join('', ['s3://', bucketName, '/', prefix, '/']);
        }
        const normalized = prefix.endsWith('/') ? prefix : `${prefix}/`;
        return cdk.Fn.join('', ['s3://', bucketName, '/', normalized]);
    }
}
