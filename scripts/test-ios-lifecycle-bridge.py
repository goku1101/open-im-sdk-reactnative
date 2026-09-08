#!/usr/bin/env python3
"""Compile the actual lifecycle bridge methods against controlled Core stubs.

Requires macOS/Xcode command line tools. This checks Objective-C callback behavior,
not the Core implementation or React Native runtime transport.
"""
from pathlib import Path
import subprocess
import tempfile

root = Path(__file__).resolve().parents[1]
source = (root / "ios/OpenImSdkRn.m").read_text()


def method(name):
    start = source.index(f"RCT_EXPORT_METHOD({name}:")
    end = source.index("\n}", start) + 2
    return source[start:end]


harness = r'''
#import <Foundation/Foundation.h>
typedef void (^RCTPromiseResolveBlock)(id);
typedef void (^RCTPromiseRejectBlock)(NSString *, NSString *, NSError *);
#define RCT_EXPORT_METHOD(method) - (void)method
static long status;
static BOOL teardownReturned;
static NSString *userID;
static NSString *lastOperationID;
static long Open_im_sdkGetLoginStatus(NSString *operationID) {
    lastOperationID = operationID;
    return status;
}
static NSString *Open_im_sdkGetLoginUserID(void) { return userID; }
static void Open_im_sdkUnInitSDK(NSString *operationID) {
    lastOperationID = operationID;
    status = 0;
    teardownReturned = YES;
}
static void check(BOOL ok, NSString *message) {
    if (!ok) { NSLog(@"FAIL: %@", message); exit(1); }
}
@interface LifecycleBridge : NSObject
@end
@implementation LifecycleBridge
__METHODS__
@end
int main(void) {
    @autoreleasepool {
        LifecycleBridge *bridge = [LifecycleBridge new];
        RCTPromiseRejectBlock reject = ^(NSString *code, NSString *message, NSError *error) {
            check(NO, @"unexpected rejection");
        };
        for (int i = 0; i < 100; i++) {
            __block int resolutions = 0;
            status = 3;
            teardownReturned = NO;
            userID = @"9007199254740993";
            [bridge getLoginStatus:@"before" resolver:^(id value) {
                check([value longValue] == 3, @"live logged-in status");
            } rejecter:reject];
            [bridge getLoginUserID:@"user" resolver:^(id value) {
                check([value isEqual:userID], @"user ID remains a string");
            } rejecter:reject];
            [bridge unInitSDK:@"teardown" resolver:^(id value) {
                resolutions++;
                check(teardownReturned, @"resolve only after Core returns");
                check(value == nil, @"same void result as Android");
            } rejecter:reject];
            check(resolutions == 1, @"unInitSDK must settle its Promise exactly once");
            check([lastOperationID isEqual:@"teardown"], @"forward operationID");
            [bridge getLoginStatus:@"after" resolver:^(id value) {
                check([value longValue] == 0, @"read latest status after teardown");
            } rejecter:reject];
        }
        NSLog(@"PASS: 100 teardown callbacks and live status reads");
    }
}
'''
harness = harness.replace("__METHODS__", "\n".join(
    method(name) for name in ("getLoginStatus", "getLoginUserID", "unInitSDK")
))
with tempfile.TemporaryDirectory(prefix="openim-lifecycle-") as directory:
    path = Path(directory)
    (path / "test.m").write_text(harness)
    subprocess.run(["xcrun", "clang", "-fobjc-arc", "-fblocks", "-framework",
                    "Foundation", str(path / "test.m"), "-o", str(path / "test")],
                   check=True)
    subprocess.run([str(path / "test")], check=True)
