#import <AppKit/AppKit.h>
#import <Foundation/Foundation.h>
#import <Vision/Vision.h>

static void EmitJSON(NSDictionary *payload) {
    NSError *error = nil;
    NSData *data = [NSJSONSerialization dataWithJSONObject:payload
                                                   options:0
                                                     error:&error];
    if (data == nil) {
        NSData *fallback = [@"{\"ok\":false,\"error\":\"json_failed\"}\n"
            dataUsingEncoding:NSUTF8StringEncoding];
        [[NSFileHandle fileHandleWithStandardOutput] writeData:fallback];
        return;
    }
    NSMutableData *output = [data mutableCopy];
    [output appendData:[@"\n" dataUsingEncoding:NSUTF8StringEncoding]];
    [[NSFileHandle fileHandleWithStandardOutput] writeData:output];
}

int main(int argc, const char *argv[]) {
    @autoreleasepool {
        if (argc != 2) {
            EmitJSON(@{@"ok": @NO, @"error": @"invalid_arguments"});
            return 2;
        }

        NSString *path = [NSString stringWithUTF8String:argv[1]];
        NSImage *image = [[NSImage alloc] initWithContentsOfFile:path];
        if (image == nil) {
            EmitJSON(@{@"ok": @NO, @"error": @"invalid_image"});
            return 3;
        }

        CGImageRef cgImage = [image CGImageForProposedRect:NULL
                                                  context:nil
                                                    hints:nil];
        if (cgImage == nil) {
            EmitJSON(@{@"ok": @NO, @"error": @"decode_failed"});
            return 4;
        }

        size_t width = CGImageGetWidth(cgImage);
        size_t height = CGImageGetHeight(cgImage);
        unsigned long long pixels =
            (unsigned long long)width * (unsigned long long)height;
        if (width == 0 || height == 0 || pixels > 25000000ULL) {
            EmitJSON(@{
                @"ok": @NO,
                @"error": @"image_dimensions_rejected",
                @"width": @(width),
                @"height": @(height),
            });
            return 5;
        }

        VNRecognizeTextRequest *request =
            [[VNRecognizeTextRequest alloc] init];
        request.recognitionLevel = VNRequestTextRecognitionLevelAccurate;
        request.usesLanguageCorrection = YES;
        request.recognitionLanguages = @[@"en-US", @"zh-Hans"];

        VNImageRequestHandler *handler =
            [[VNImageRequestHandler alloc] initWithCGImage:cgImage
                                                   options:@{}];
        NSError *visionError = nil;
        BOOL succeeded = [handler performRequests:@[request]
                                            error:&visionError];
        if (!succeeded) {
            EmitJSON(@{@"ok": @NO, @"error": @"vision_failed"});
            return 6;
        }

        NSMutableArray *lines = [NSMutableArray array];
        for (VNRecognizedTextObservation *observation in request.results) {
            VNRecognizedText *candidate =
                [[observation topCandidates:1] firstObject];
            if (candidate == nil || candidate.string.length == 0) {
                continue;
            }
            [lines addObject:@{
                @"text": candidate.string,
                @"confidence": @(candidate.confidence),
            }];
            if (lines.count >= 200) {
                break;
            }
        }

        EmitJSON(@{
            @"ok": @YES,
            @"width": @(width),
            @"height": @(height),
            @"lines": lines,
        });
        return 0;
    }
}
