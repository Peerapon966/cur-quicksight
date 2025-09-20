import * as fs from 'fs';
import * as path from 'path';
import * as cdk from 'aws-cdk-lib';

export type JsonValue = string | number | boolean | null | JsonValue[] | { [key: string]: JsonValue } | cdk.IResolvable;

export function loadJsonTemplate(relativePath: string): any {
    const absolute = path.resolve(relativePath);
    const buffer = fs.readFileSync(absolute, 'utf8');
    return JSON.parse(buffer);
}

export type JsonVisitor = (value: any, pathSegments: Array<string | number>) => void;

export function visitJson(value: any, visitor: JsonVisitor, pathSegments: Array<string | number> = []): void {
    visitor(value, pathSegments);

    if (Array.isArray(value)) {
        value.forEach((item, index) => visitJson(item, visitor, [...pathSegments, index]));
        return;
    }

    if (value && typeof value === 'object' && !cdk.Token.isUnresolved(value)) {
        Object.keys(value).forEach((key) => {
            visitJson(value[key], visitor, [...pathSegments, key]);
        });
    }
}

export function assertNoHardCodedValues(value: any, messageContext: string): void {
    visitJson(value, (node, pathSegments) => {
        if (typeof node !== 'string') {
            return;
        }

        if (cdk.Token.isUnresolved(node)) {
            return;
        }

        if (/arn:aws:[^:]+:[^:]+:\d{12}:/i.test(node)) {
            throw new Error(`Hardcoded ARN detected at ${pathSegments.join('.')} in ${messageContext}`);
        }

        if (node.includes('s3://') && !node.includes('${Token[')) {
            throw new Error(`Hardcoded S3 URI detected at ${pathSegments.join('.')} in ${messageContext}`);
        }
    });
}

export function deepClone<T>(value: T): T {
    return JSON.parse(JSON.stringify(value));
}
