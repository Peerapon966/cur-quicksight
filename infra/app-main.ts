#!/usr/bin/env node
import { AppContext, AppContextError } from '../lib/template/app-context';

import { StorageStack } from './stack/storage-stack';
import { GlueStack } from './stack/glue-stack';
import { AthenaStack } from './stack/athena-stack';
import { QuicksightStack } from './stack/quicksight-stack';
import { ComputeStack } from './stack/compute-stack';


try {
    const appContext = new AppContext({
        appConfigFileKey: 'APP_CONFIG',
    });

    const storageStack = new StorageStack(appContext, appContext.appConfig.Stack.Storage);

    const glueStack = new GlueStack(appContext, appContext.appConfig.Stack.Glue, {
        curExportBucket: storageStack.curExportBucket,
        partnerListBucket: storageStack.partnerListBucket,
    });
    glueStack.addDependency(storageStack);

    const athenaStack = new AthenaStack(appContext, appContext.appConfig.Stack.Athena);
    athenaStack.addDependency(glueStack);

    const quicksightStack = new QuicksightStack(appContext, appContext.appConfig.Stack.Quicksight, {
        athenaStack,
    });
    quicksightStack.addDependency(glueStack);
    quicksightStack.addDependency(athenaStack);

    const computeStack = new ComputeStack(appContext, appContext.appConfig.Stack.Compute, {
        partnerListBucket: storageStack.partnerListBucket,
        quicksightStack,
        athenaStack,
    });
    computeStack.addDependency(storageStack);
    computeStack.addDependency(quicksightStack);
    computeStack.addDependency(athenaStack);
} catch (error) {
    if (error instanceof AppContextError) {
        console.error('[AppContextError]:', error.message);
    } else {
        console.error('[Error]: not-handled-error', error);
    }
}
