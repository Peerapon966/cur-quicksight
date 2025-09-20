import * as cdk from 'aws-cdk-lib';
import { BaseStack } from './template/stack/base/base-stack';

export interface ParameterDefinition {
    readonly parameter: cdk.CfnParameter;
    readonly value: string;
    readonly resolved?: string;
}

export interface SharedParameters {
    readonly curExportBucketName: ParameterDefinition;
    readonly partnerListBucketName: ParameterDefinition;
    readonly glueDatabaseName: ParameterDefinition;
    readonly createGlueCrawlers: ParameterDefinition;
    readonly curTableName: ParameterDefinition;
    readonly quicksightNamespace: ParameterDefinition;
    readonly quicksightAdminUserArn: ParameterDefinition;
    readonly rlsAthenaResultsBucketName: ParameterDefinition;
    readonly rlsAthenaResultsPrefix: ParameterDefinition;
    readonly dailyRefreshTimeLocal: ParameterDefinition;
    readonly timezone: ParameterDefinition;
}

interface ParameterSources {
    readonly global?: Record<string, any>;
    readonly stack?: Record<string, any>;
}

function lookupFromConfig(sources: ParameterSources, key: string): string | undefined {
    const fromStack = sources.stack && sources.stack[key];
    if (typeof fromStack === 'string' && fromStack.length > 0) {
        return fromStack;
    }
    const fromGlobal = sources.global && sources.global[key];
    if (typeof fromGlobal === 'string' && fromGlobal.length > 0) {
        return fromGlobal;
    }
    return undefined;
}

function createStringParameter(stack: BaseStack, id: string, props: cdk.CfnParameterProps, sources: ParameterSources): ParameterDefinition {
    const parameterProps: cdk.CfnParameterProps = {
        type: 'String',
        ...props,
    };

    const param = new cdk.CfnParameter(stack, id, parameterProps);

    const contextValue = stack.node.tryGetContext(id);
    const configValue = lookupFromConfig(sources, id);
    const overrideValue = contextValue ?? configValue;

    if (overrideValue !== undefined && overrideValue !== null && overrideValue !== '') {
        param.default = overrideValue;
    }

    const providedDefault = parameterProps.default;
    const resolved = typeof overrideValue === 'string' && overrideValue.length > 0
        ? overrideValue
        : (typeof providedDefault === 'string' && providedDefault.length > 0 ? providedDefault : undefined);

    return {
        parameter: param,
        value: param.valueAsString,
        resolved,
    };
}

export interface ParameterSourceOverrides {
    readonly globalParameters?: Record<string, any>;
    readonly stackParameters?: Record<string, any>;
}

export function createSharedParameters(stack: BaseStack, overrides?: ParameterSourceOverrides): SharedParameters {
    const sources: ParameterSources = {
        global: overrides?.globalParameters,
        stack: overrides?.stackParameters,
    };

    const glueDatabaseDefault = 'cur-quicksight';
    const rlsPrefixDefault = 'cur-rls/';
    const dailyRefreshDefault = '10:00';
    const timezoneDefault = 'Asia/Bangkok';

    const curExportBucketName = createStringParameter(stack, 'CurExportBucketName', {
        description: 'S3 bucket name where CUR exports are delivered.',
        minLength: 3
    }, sources);

    const partnerListBucketName = createStringParameter(stack, 'PartnerListBucketName', {
        description: 'S3 bucket name for partner list files.',
        minLength: 3
    }, sources);

    const glueDatabaseName = createStringParameter(stack, 'GlueDatabaseName', {
        default: glueDatabaseDefault,
        description: 'Name of the AWS Glue database hosting CUR metadata.'
    }, sources);

    const createGlueCrawlers = createStringParameter(stack, 'CreateGlueCrawlers', {
        default: 'true',
        allowedValues: ['true', 'false'],
        description: 'Set to false to create Glue tables directly instead of crawlers.'
    }, sources);

    const curTableName = createStringParameter(stack, 'CurTableName', {
        default: 'data',
        description: 'Name of the CUR Glue table when crawlers are disabled.'
    }, sources);

    const quicksightNamespace = createStringParameter(stack, 'QuicksightNamespace', {
        default: 'default',
        description: 'QuickSight namespace where resources will reside.'
    }, sources);

    const quicksightAdminUserArn = createStringParameter(stack, 'QuicksightAdminUserArn', {
        description: 'ARN of the QuickSight user or group granted administrative access.'
    }, sources);

    const rlsAthenaResultsBucketName = createStringParameter(stack, 'RlsAthenaResultsBucketName', {
        default: cdk.Fn.sub('aws-athena-query-results-${AWS::Region}-${AWS::AccountId}'),
        description: 'Bucket for Athena query results used during RLS updates.'
    }, sources);

    const rlsAthenaResultsPrefix = createStringParameter(stack, 'RlsAthenaResultsPrefix', {
        default: rlsPrefixDefault,
        description: 'Prefix within the Athena results bucket to store temporary files.'
    }, sources);

    const dailyRefreshTimeLocal = createStringParameter(stack, 'DailyRefreshTimeLocal', {
        default: dailyRefreshDefault,
        description: 'Local time (HH:MM) to trigger QuickSight refresh operations.'
    }, sources);

    const timezone = createStringParameter(stack, 'Timezone', {
        default: timezoneDefault,
        description: 'IANA timezone identifier representing the local time zone.'
    }, sources);

    return {
        curExportBucketName,
        partnerListBucketName,
        glueDatabaseName,
        createGlueCrawlers,
        curTableName,
        quicksightNamespace,
        quicksightAdminUserArn,
        rlsAthenaResultsBucketName,
        rlsAthenaResultsPrefix,
        dailyRefreshTimeLocal,
        timezone
    };
}
