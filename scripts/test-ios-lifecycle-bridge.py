#!/usr/bin/env python3
"""Exercise actual bridge and callback code with deferred Core callbacks.

This proves bridge ordering and rejection, not Core goroutine termination.
Requires macOS/Xcode command line tools.
"""
from pathlib import Path
import subprocess
import tempfile

root = Path(__file__).resolve().parents[1]
source = (root / 'ios/OpenImSdkRn.m').read_text()

def method(name):
    start = source.index(f'RCT_EXPORT_METHOD({name}:')
    return source[start:source.index('\n}', start) + 2]

helpers = source[source.index('static BOOL lifecycleInFlight'):source.index('- (dispatch_queue_t)methodQueue')]
proxy = (root / 'ios/CallbackProxy.m').read_text().replace('#import "CallbackProxy.h"', '')
harness = r'''
#import <Foundation/Foundation.h>
#import <dispatch/dispatch.h>
typedef void (^RCTPromiseResolveBlock)(id);
typedef void (^RCTPromiseRejectBlock)(NSString *, NSString *, NSError *);
typedef id (^RNOIMSuccessCallback)(NSString *);
@interface RNCallbackProxy : NSObject
- (id)initWithCallback:(RCTPromiseResolveBlock)resolver rejecter:(RCTPromiseRejectBlock)rejecter;
- (id)initWithCallback:(RCTPromiseResolveBlock)resolver rejecter:(RCTPromiseRejectBlock)rejecter onSuccess:(RNOIMSuccessCallback)onSuccess;
- (void)onSuccess:(NSString *)data;
- (void)onError:(int32_t)code errMsg:(NSString *)message;
@end
__PROXY__
@interface NSDictionary (JSON)
- (NSString *)json;
@end
@implementation NSDictionary (JSON)
- (NSString *)json { return @"{}"; }
@end
#define RCT_EXPORT_METHOD(method) - (void)method
static long status = 1;
static int uninitCalls, initCalls, loginCalls, logoutCalls;
static RNCallbackProxy *pending;
static long Open_im_sdkGetLoginStatus(NSString *op) { return status; }
static NSString *Open_im_sdkGetLoginUserID(void) { return @"9007199254740993"; }
static void Open_im_sdkUnInitSDK(NSString *op) { uninitCalls++; status = -1001; }
static BOOL Open_im_sdkInitSDK(id listener, NSString *op, NSString *config) { initCalls++; status = 1; return YES; }
static void Open_im_sdkLogin(RNCallbackProxy *proxy, NSString *op, NSString *uid, NSString *token) {
    loginCalls++; pending = proxy; status = 2;
}
static void Open_im_sdkLogout(RNCallbackProxy *proxy, NSString *op) { logoutCalls++; pending = proxy; }
#define Open_im_sdkSetUserListener(x)
#define Open_im_sdkSetConversationListener(x)
#define Open_im_sdkSetFriendListener(x)
#define Open_im_sdkSetGroupListener(x)
#define Open_im_sdkSetAdvancedMsgListener(x)
#define Open_im_sdkSetBatchMsgListener(x)
#define Open_im_sdkSetCustomBusinessListener(x)
static void check(BOOL ok, NSString *msg) { if (!ok) { NSLog(@"FAIL: %@", msg); exit(1); } }
static void drain(void) { [[NSRunLoop mainRunLoop] runUntilDate:[NSDate dateWithTimeIntervalSinceNow:0.001]]; }
@interface LifecycleBridge : NSObject
@end
@implementation LifecycleBridge
__HELPERS__
__METHODS__
@end
int main(void) {
    @autoreleasepool {
        LifecycleBridge *bridge = [LifecycleBridge new];
        RCTPromiseResolveBlock unexpectedResolve = ^(id value) { check(NO, @"must reject unsafe lifecycle call"); };
        RCTPromiseRejectBlock unexpectedReject = ^(NSString *code, NSString *msg, NSError *err) { check(NO, msg); };
        RCTPromiseRejectBlock busy = ^(NSString *code, NSString *msg, NSError *err) {
            check([code isEqual:@"OPENIM_LIFECYCLE_BUSY"], @"busy rejection");
        };
        for (int i = 0; i < 100; i++) {
            __block int resolutions = 0;
            [bridge initSDK:@{} operationID:@"init" resolver:^(id v) {} rejecter:unexpectedReject];
            [bridge login:@{@"userID":@"9007199254740993", @"token":@"test"} operationID:@"login"
                resolver:^(id v) { resolutions++; } rejecter:unexpectedReject];
            check(resolutions == 0, @"login waits for Core callback");
            [bridge unInitSDK:@"early" resolver:unexpectedResolve rejecter:busy];
            [bridge logout:@"early" resolver:unexpectedResolve rejecter:busy];
            [bridge initSDK:@{} operationID:@"early" resolver:unexpectedResolve rejecter:busy];
            check(initCalls == i + 1 && uninitCalls == i, @"busy calls never enter Core");
            status = 3;
            RNCallbackProxy *oldLogin = pending;
            [pending onSuccess:nil]; drain();
            check(resolutions == 1, @"login settles once after completion");
            [bridge unInitSDK:@"logged-in" resolver:unexpectedResolve rejecter:^(NSString *code, NSString *msg, NSError *err) {
                check([code isEqual:@"OPENIM_LOGOUT_REQUIRED"], @"logged-in singleton cannot be discarded");
            }];
            [bridge logout:@"logout" resolver:^(id v) { resolutions++; } rejecter:unexpectedReject];
            [oldLogin onSuccess:nil]; drain();
            [bridge login:@{} operationID:@"early" resolver:unexpectedResolve rejecter:busy];
            [bridge unInitSDK:@"early" resolver:unexpectedResolve rejecter:busy];
            check(resolutions == 1, @"logout remains pending and duplicate callback does not unlock it");
            status = 1;
            [pending onSuccess:nil]; drain();
            check(resolutions == 2, @"logout waits for callback");
            [bridge unInitSDK:@"done" resolver:^(id v) {
                check(status == -1001 && v == nil, @"uninit resolves after Core returns");
            } rejecter:unexpectedReject];
            [bridge getLoginStatus:@"status" resolver:^(id v) { check([v longValue] == -1001, @"live status"); } rejecter:unexpectedReject];
            [bridge getLoginUserID:@"uid" resolver:^(id v) { check([v isEqual:@"9007199254740993"], @"string ID preserved"); } rejecter:unexpectedReject];
        }
        __block BOOL failed = NO;
        [bridge login:@{} operationID:@"failure" resolver:unexpectedResolve rejecter:^(NSString *code, NSString *msg, NSError *err) { failed = YES; }];
        status = 1;
        [pending onError:500 errMsg:@"failed"]; drain();
        check(failed, @"failure settles and releases gate");
        [bridge unInitSDK:@"after-failure" resolver:^(id v) {} rejecter:unexpectedReject];
        check(loginCalls == 101 && logoutCalls == 100, @"no overlapping native calls");
        NSLog(@"PASS: 100 deferred login/logout/uninit cycles, busy guards, duplicate callbacks and error recovery");
    }
}
'''
harness = harness.replace('__PROXY__', proxy).replace('__HELPERS__', helpers).replace('__METHODS__', '\n'.join(method(n) for n in ('initSDK', 'login', 'logout', 'getLoginStatus', 'getLoginUserID', 'unInitSDK')))
with tempfile.TemporaryDirectory(prefix='openim-lifecycle-') as directory:
    path = Path(directory)
    (path / 'test.m').write_text(harness)
    subprocess.run(['xcrun', 'clang', '-fobjc-arc', '-fblocks', '-framework', 'Foundation', str(path / 'test.m'), '-o', str(path / 'test')], check=True)
    subprocess.run([str(path / 'test')], check=True)
