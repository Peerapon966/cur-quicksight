import { S3Event, S3EventRecord } from 'aws-lambda';
import { DescribeStateMachineCommand, SFNClient, StartExecutionCommand } from '@aws-sdk/client-sfn';
import { GetCallerIdentityCommand, STSClient } from '@aws-sdk/client-sts';

interface ExecutionPayload {
    readonly bucket: string;
    readonly key: string;
    readonly size: number | undefined;
    readonly etag: string | undefined;
    readonly eventTime: string;
}

const region = process.env.REGION ?? process.env.AWS_REGION;
if (!region) {
    throw new Error('REGION environment variable must be defined');
}

const stateMachineNameEnv = process.env.STATE_MACHINE_NAME;
if (!stateMachineNameEnv) {
    throw new Error('STATE_MACHINE_NAME environment variable must be defined');
}

const sfnClient = new SFNClient({ region });
const stsClient = new STSClient({ region });
let cachedStateMachineArn: string | undefined;

async function resolveStateMachineArn(nameOrArn: string): Promise<string> {
    if (cachedStateMachineArn) {
        return cachedStateMachineArn;
    }

    if (nameOrArn.startsWith('arn:')) {
        cachedStateMachineArn = nameOrArn;
        return cachedStateMachineArn;
    }

    const identity = await stsClient.send(new GetCallerIdentityCommand({}));
    if (!identity.Account) {
        throw new Error('Unable to determine AWS account ID for Step Functions execution');
    }

    const arn = `arn:aws:states:${region}:${identity.Account}:stateMachine:${nameOrArn}`;
    await sfnClient.send(new DescribeStateMachineCommand({ stateMachineArn: arn }));
    cachedStateMachineArn = arn;
    return arn;
}

function shouldProcessRecord(record: S3EventRecord): boolean {
    const key = decodeURIComponent(record.s3.object.key.replace(/\+/g, ' '));
    return key.startsWith('IT info/');
}

function buildPayload(record: S3EventRecord): ExecutionPayload {
    const decodedKey = decodeURIComponent(record.s3.object.key.replace(/\+/g, ' '));
    return {
        bucket: record.s3.bucket.name,
        key: decodedKey,
        size: record.s3.object.size,
        etag: record.s3.object.eTag,
        eventTime: record.eventTime,
    };
}

export const handler = async (event: S3Event): Promise<void> => {
    const stateMachineArn = await resolveStateMachineArn(stateMachineNameEnv);

    for (const record of event.Records ?? []) {
        if (!shouldProcessRecord(record)) {
            console.info('Skipping object outside of monitored prefix', {
                bucket: record.s3.bucket.name,
                key: record.s3.object.key,
            });
            continue;
        }

        const payload = buildPayload(record);
        console.info('Starting Step Functions execution', { stateMachineArn, payload });

        await sfnClient.send(new StartExecutionCommand({
            stateMachineArn,
            input: JSON.stringify(payload),
        }));
    }
};
