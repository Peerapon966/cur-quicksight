import * as cdk from 'aws-cdk-lib';
import { assertNoHardCodedValues } from '../lib/util/json-template';

describe('json-template sanitization', () => {
    test('throws when hard-coded arn detected', () => {
        expect(() => assertNoHardCodedValues({ value: 'arn:aws:quicksight:us-east-1:123456789012:dataset/example' }, 'test')).toThrow();
    });

    test('allows tokenized arns', () => {
        const app = new cdk.App();
        const stack = new cdk.Stack(app);
        const value = stack.formatArn({
            service: 'quicksight',
            resource: 'dataset',
            resourceName: 'example',
        });

        expect(() => assertNoHardCodedValues({ value }, 'test')).not.toThrow();
    });
});
