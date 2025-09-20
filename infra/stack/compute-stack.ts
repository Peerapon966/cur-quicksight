import * as path from 'path';
import * as cdk from 'aws-cdk-lib';
import * as iam from 'aws-cdk-lib/aws-iam';
import * as lambda from 'aws-cdk-lib/aws-lambda';
import * as lambdaNodejs from 'aws-cdk-lib/aws-lambda-nodejs';
import * as logs from 'aws-cdk-lib/aws-logs';
import * as cr from 'aws-cdk-lib/custom-resources';
import * as s3 from 'aws-cdk-lib/aws-s3';
import * as stepfunctions from 'aws-cdk-lib/aws-stepfunctions';

import { AppContext } from '../../lib/template/app-context';
import { StackConfig } from '../../lib/template/app-config';
import { BaseStack } from '../../lib/template/stack/base/base-stack';
import { createSharedParameters, SharedParameters } from '../../lib/cur-parameters';
import { assertNoHardCodedValues, deepClone, loadJsonTemplate } from '../../lib/util/json-template';
import { AthenaStack } from './athena-stack';
import { QuicksightStack } from './quicksight-stack';

const NODEJS_20_RUNTIME = new lambda.Runtime('nodejs20.x', lambda.RuntimeFamily.NODEJS);

export interface ComputeStackProps {
    readonly partnerListBucket: s3.Bucket;
    readonly quicksightStack: QuicksightStack;
    readonly athenaStack: AthenaStack;
}

export class ComputeStack extends BaseStack {
    public readonly parameters: SharedParameters;
    public readonly stateMachine: stepfunctions.CfnStateMachine;
    public readonly handler: lambdaNodejs.NodejsFunction;

    constructor(appContext: AppContext, stackConfig: StackConfig, private readonly props: ComputeStackProps) {
        super(appContext, stackConfig);

        this.parameters = createSharedParameters(this, {
            globalParameters: this.commonProps.appConfig?.Global?.Parameters,
            stackParameters: this.stackConfig?.Parameters,
        });

        const logGroup = new logs.LogGroup(this, 'StateMachineLogs', {
            retention: logs.RetentionDays.ONE_MONTH,
            removalPolicy: cdk.RemovalPolicy.RETAIN,
        });

        const stateMachineRole = this.createStateMachineRole();

        const definition = this.buildStateMachineDefinition();

        this.stateMachine = new stepfunctions.CfnStateMachine(this, 'UpdateCurQuicksightRls', {
            stateMachineName: 'update-CUR-Quicksight-RLS',
            roleArn: stateMachineRole.roleArn,
            definition,
            loggingConfiguration: {
                destinations: [
                    {
                        cloudWatchLogsLogGroup: {
                            logGroupArn: logGroup.logGroupArn,
                        },
                    },
                ],
                includeExecutionData: true,
                level: 'ERROR',
            },
            tracingConfiguration: {
                enabled: true,
            },
        });
        const stateMachineRoleResource = stateMachineRole.node.defaultChild as cdk.CfnResource;
        this.stateMachine.addDependency(stateMachineRoleResource);

        this.handler = new lambdaNodejs.NodejsFunction(this, 'ItInfoEventHandler', {
            entry: path.join(__dirname, '../../src/lambdas/itInfoEventHandler.ts'),
            handler: 'handler',
            runtime: NODEJS_20_RUNTIME,
            architecture: lambda.Architecture.ARM_64,
            memorySize: 256,
            timeout: cdk.Duration.seconds(60),
            environment: {
                STATE_MACHINE_NAME: this.stateMachine.ref,
                REGION: cdk.Aws.REGION,
            },
            bundling: {
                minify: true,
                sourceMap: true,
                target: 'node20',
                keepNames: true,
            },
        });

        this.handler.addToRolePolicy(new iam.PolicyStatement({
            actions: ['states:StartExecution', 'states:DescribeStateMachine'],
            resources: [this.stateMachine.attrArn],
        }));

        this.handler.addToRolePolicy(new iam.PolicyStatement({
            actions: ['s3:GetObject'],
            resources: [this.props.partnerListBucket.arnForObjects('IT info/*')],
        }));

        this.handler.addPermission('AllowPartnerBucketInvoke', {
            action: 'lambda:InvokeFunction',
            principal: new iam.ServicePrincipal('s3.amazonaws.com'),
            sourceArn: this.props.partnerListBucket.bucketArn,
        });

        const notificationResource = new cr.AwsCustomResource(this, 'PartnerBucketNotification', {
            policy: cr.AwsCustomResourcePolicy.fromSdkCalls({
                resources: [this.props.partnerListBucket.bucketArn],
            }),
            installLatestAwsSdk: false,
            onCreate: {
                service: 'S3',
                action: 'putBucketNotificationConfiguration',
                parameters: this.buildNotificationConfiguration(),
                physicalResourceId: cr.PhysicalResourceId.of('PartnerBucketNotification'),
            },
            onUpdate: {
                service: 'S3',
                action: 'putBucketNotificationConfiguration',
                parameters: this.buildNotificationConfiguration(),
            },
            onDelete: {
                service: 'S3',
                action: 'putBucketNotificationConfiguration',
                parameters: {
                    Bucket: this.props.partnerListBucket.bucketName,
                    NotificationConfiguration: {},
                },
            },
        });
        notificationResource.node.addDependency(this.handler);
        notificationResource.node.addDependency(this.props.partnerListBucket);

        this.exportOutput('StateMachineArn', this.stateMachine.attrArn);
    }

    private createStateMachineRole(): iam.Role {
        const role = new iam.Role(this, 'StateMachineRole', {
            assumedBy: new iam.ServicePrincipal('states.amazonaws.com'),
        });

        role.addToPolicy(new iam.PolicyStatement({
            actions: ['athena:StartQueryExecution', 'athena:GetQueryExecution', 'athena:GetQueryResults'],
            resources: [
                this.props.athenaStack.workGroupArn(this.props.athenaStack.curWorkGroupName),
                this.props.athenaStack.workGroupArn(this.props.athenaStack.rlsWorkGroupName),
            ],
        }));

        const partnerBucketObjects = this.props.partnerListBucket.arnForObjects('RLS/*');
        const resultsPrefix = this.ensureTrailingSlash(this.parameters.rlsAthenaResultsPrefix.value);
        const resultsObjectsArn = cdk.Fn.join('', [
            'arn:',
            cdk.Aws.PARTITION,
            ':s3:::',
            this.parameters.rlsAthenaResultsBucketName.value,
            '/',
            resultsPrefix,
            '*',
        ]);

        role.addToPolicy(new iam.PolicyStatement({
            actions: ['s3:CopyObject', 's3:GetObject', 's3:PutObject'],
            resources: [partnerBucketObjects, resultsObjectsArn],
        }));

        role.addToPolicy(new iam.PolicyStatement({
            actions: ['quicksight:ListGroups'],
            resources: ['*'],
        }));

        const namespaceResourceArn = cdk.Stack.of(this).formatArn({
            service: 'quicksight',
            resource: 'group',
            resourceName: `${this.parameters.quicksightNamespace.value}/*`,
        });

        role.addToPolicy(new iam.PolicyStatement({
            actions: ['quicksight:CreateGroup'],
            resources: [namespaceResourceArn],
        }));

        role.addToPolicy(new iam.PolicyStatement({
            actions: ['quicksight:CreateIngestion'],
            resources: [this.props.quicksightStack.curDataSet.attrArn],
        }));

        role.addToPolicy(new iam.PolicyStatement({
            actions: [
                'logs:CreateLogDelivery',
                'logs:GetLogDelivery',
                'logs:UpdateLogDelivery',
                'logs:DeleteLogDelivery',
                'logs:ListLogDeliveries',
                'logs:PutResourcePolicy',
                'logs:DescribeResourcePolicies',
                'logs:DescribeLogGroups',
            ],
            resources: ['*'],
        }));

        return role;
    }

    private buildStateMachineDefinition(): any {
        const definition = deepClone(loadJsonTemplate('assets/step_functions/update-cur-quicksight-rls.json'));

        definition.States['Query New Partner List'].Arguments.QueryString = this.renderSql(
            definition.States['Query New Partner List'].Arguments.QueryString
        );
        definition.States['Query New Partner List'].Arguments.WorkGroup = this.props.athenaStack.rlsWorkGroupName;

        const parallelBranches = definition.States['Parallel'].Branches;
        const listGroupsState = parallelBranches[0]
            .States['Get Non-exist Quicksight Group']
            .Branches[0]
            .States['ListGroups'];
        listGroupsState.Arguments.AwsAccountId = cdk.Aws.ACCOUNT_ID;
        listGroupsState.Arguments.Namespace = this.parameters.quicksightNamespace.value;

        const createGroupState = parallelBranches[0]
            .States['Map']
            .ItemProcessor
            .States['Create Group'];
        createGroupState.Arguments.AwsAccountId = cdk.Aws.ACCOUNT_ID;
        createGroupState.Arguments.Namespace = this.parameters.quicksightNamespace.value;

        const copyPartnerListState = parallelBranches[1].States['Copy Partner List'];
        copyPartnerListState.Arguments.Bucket = this.props.partnerListBucket.bucketName;
        copyPartnerListState.Arguments.CopySource = this.buildCopySourceExpression();
        copyPartnerListState.Arguments.Key = 'RLS/dataset-rules.csv';

        const createIngestionState = parallelBranches[1].States['Create Ingestion'];
        createIngestionState.Arguments.AwsAccountId = cdk.Aws.ACCOUNT_ID;
        createIngestionState.Arguments.DataSetId = this.props.quicksightStack.curDataSetId;

        assertNoHardCodedValues(definition, 'update-cur-quicksight-rls');
        return definition;
    }

    private renderSql(query: string): string {
        return query.replace(/"cur-quicksight"/g, `"${this.parameters.glueDatabaseName.value}"`);
    }

    private buildCopySourceExpression(): string {
        const prefix = this.ensureTrailingSlash(this.parameters.rlsAthenaResultsPrefix.value);
        return cdk.Fn.join('', [
            '{% "',
            this.parameters.rlsAthenaResultsBucketName.value,
            '/',
            prefix,
            '" & $executionID & ".csv" %}'
        ]);
    }

    private ensureTrailingSlash(value: string): string {
        if (cdk.Token.isUnresolved(value)) {
            return cdk.Fn.join('', [value, '/']);
        }
        return value.endsWith('/') ? value : `${value}/`;
    }

    private buildNotificationConfiguration(): any {
        return {
            Bucket: this.props.partnerListBucket.bucketName,
            NotificationConfiguration: {
                LambdaFunctionConfigurations: [
                    {
                        Events: ['s3:ObjectCreated:*'],
                        LambdaFunctionArn: this.handler.functionArn,
                        Filter: {
                            Key: {
                                FilterRules: [
                                    { Name: 'prefix', Value: 'IT info/' },
                                ],
                            },
                        },
                    },
                ],
            },
        };
    }
}
