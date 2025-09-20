import * as cdk from 'aws-cdk-lib';
import * as s3 from 'aws-cdk-lib/aws-s3';
import * as s3deploy from 'aws-cdk-lib/aws-s3-deployment';

import { AppContext } from '../../lib/template/app-context';
import { StackConfig } from '../../lib/template/app-config';
import { BaseStack } from '../../lib/template/stack/base/base-stack';
import { createSharedParameters, SharedParameters } from '../../lib/cur-parameters';

export interface StorageStackProps {
}

export class StorageStack extends BaseStack {
    public readonly parameters: SharedParameters;
    public readonly curExportBucket: s3.Bucket;
    public readonly partnerListBucket: s3.Bucket;

    constructor(appContext: AppContext, stackConfig: StackConfig, _props?: StorageStackProps) {
        super(appContext, stackConfig);

        this.parameters = createSharedParameters(this, {
            globalParameters: this.commonProps.appConfig?.Global?.Parameters,
            stackParameters: this.stackConfig?.Parameters,
        });

        this.curExportBucket = new s3.Bucket(this, 'CurExportBucket', {
            bucketName: this.parameters.curExportBucketName.value,
            blockPublicAccess: s3.BlockPublicAccess.BLOCK_ALL,
            encryption: s3.BucketEncryption.S3_MANAGED,
            enforceSSL: true,
            removalPolicy: cdk.RemovalPolicy.RETAIN,
        });

        this.partnerListBucket = new s3.Bucket(this, 'PartnerListBucket', {
            bucketName: this.parameters.partnerListBucketName.value,
            blockPublicAccess: s3.BlockPublicAccess.BLOCK_ALL,
            encryption: s3.BucketEncryption.S3_MANAGED,
            enforceSSL: true,
            removalPolicy: cdk.RemovalPolicy.RETAIN,
        });

        new s3deploy.BucketDeployment(this, 'PartnerListPrefixes', {
            destinationBucket: this.partnerListBucket,
            sources: [
                s3deploy.Source.data('IT info/.keep', ''),
                s3deploy.Source.data('RLS/.keep', ''),
            ],
        });

        this.exportOutput('CurExportBucketName', this.curExportBucket.bucketName);
        this.exportOutput('PartnerListBucketName', this.partnerListBucket.bucketName);
    }
}
