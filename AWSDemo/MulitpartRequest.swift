//
//  MulitpartRequest.swift
//  AWSDemo
//
//  Created by Milan Aggarwal on 14/10/21.
//

import AWSCore
import AWSAuthCore
import AWSS3
import CommonCrypto

class MultipartRequest: NSObject, URLSessionDelegate {
    
    let accessKey = "AKIA5YHCYZAHS7BIS25U"
    let secretKey = "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"
    let bucketName = "videostestdummy"
    let fileName = "multiUploadFile"
    let contentType = "video/MP4"
    let fileURL: URL
    var chunkSize = 5 * 1024 * 1024
    var session: URLSession?
    var videoURLs : [URL] = []
    let opQueue = OperationQueue()
    let newFileName = "test"
    
    var multipartUploadId: String?
    var completedPartsInfo: AWSS3CompletedMultipartUpload?
    
    init(fileURL: URL, isSingle: Bool = false) {
        self.fileURL = fileURL
        super.init()
        opQueue.maxConcurrentOperationCount = 1
        self.session = URLSession.init(configuration: .default, delegate: self, delegateQueue: opQueue)
        if isSingle {
            chunkSize = fileURL.getFileSize()
        }
        self.videoURLs = splitVideoUrl(fileURL, count: fileURL.getChunkCount(chunckSize: chunkSize), chunkSize: chunkSize)
        
        //Create a credentialsProvider to instruct AWS how to sign those URL
        let credentialsProvider = AWSStaticCredentialsProvider(accessKey: accessKey, secretKey: secretKey)
         
         //create a service configuration with the credential provider we just created
        let configuration = AWSServiceConfiguration.init(region: AWSRegionType.USEast2, credentialsProvider: credentialsProvider)

        //set this as the default configuration
        //this way any time the AWS frameworks needs to get credential
        //information, it will get those from the credential provider we just created
        AWSServiceManager.default().defaultServiceConfiguration = configuration
    }
    
    private func splitVideoUrl(_ videoUrl: URL, count: Int, chunkSize: Int) -> [URL] {
        var urls : [URL] = []
        if count == 1 {
            return [videoUrl]
        }
        let chunkSize = chunkSize
        let filePath = videoUrl.path
        
        let paths = FileManager.default.urls(for: .documentDirectory, in: .userDomainMask)
            let documentsDirectory = paths[0]
        let newFilePath = documentsDirectory.appendingPathComponent(newFileName)
        
        if let stream = InputStream(fileAtPath: filePath) {
            stream.open()
            var i = 1
            
            let buffer = UnsafeMutablePointer<UInt8>.allocate(capacity: chunkSize)
            defer {
                buffer.deallocate()
            }
            while stream.hasBytesAvailable {
                var data = Data()
                let read = stream.read(buffer, maxLength: chunkSize)
                if read < 0 {
                    //Stream error occured
                } else if read == 0 {
                    //EOF
                    break
                } else {
                    data.append(buffer, count: read)
                    //                    if let data = try? Data(reading: stream, bufferSize: chunkSize) {
                    let newFile = "\(newFilePath.path)_\(i)"
                    let result = FileManager.default.createFile(atPath: newFile, contents: data, attributes: [:])
                    print("file chunk \(i) written : \(result)")
                    urls.append(URL(fileURLWithPath: newFile))
                    //                    }
                }
                i = i + 1
            }
            stream.close()
        }
        print(urls)
        return urls
    }
    
    func start() {
        //create a request to start a multipart upload
        guard let multipartRequest = AWSS3CreateMultipartUploadRequest() else { return }

        //the key in AWS S3 parlance is the name of the file, it needs to be unique
        multipartRequest.key = fileName

        //tell which bucket you want to upload to
        multipartRequest.bucket = bucketName

        //and the content type of the file you are uploading (in my case MP4 video)
        multipartRequest.contentType = contentType
    
        //access the default AWS S3 object, which is configured appropriately
        let awsService = AWSS3.default()

        //actually create the multipart upload using the multipart request we created earlier
        awsService.createMultipartUpload(multipartRequest).continueWith(block: { task in
            //get the ID that AWS uses to uniquely identify this upload as you'll need it later
            let output:AWSS3CreateMultipartUploadOutput = task.result!
            
            self.multipartUploadId = output.uploadId
                        
            //as individual part complete you'll want to keep track of those
            //as AWS S3 requires the list of all parts to be able to reassemble the file
            self.completedPartsInfo = AWSS3CompletedMultipartUpload()
            self.completedPartsInfo?.parts = []
            //now that we have an upload ID we can actually start uploading the parts
            self.uploadAllParts()
            
            return nil
        })
    }
    
    func uploadAllParts ()
    {
        //get the file size of the file to upload
        let fileAttributes = try! FileManager.default.attributesOfItem(atPath: self.fileURL.path)
        let fileSizeNumber = fileAttributes[FileAttributeKey.size] as! NSNumber
        let fileSize = fileSizeNumber.int64Value

        //figure out how many parts we're going to have
        let partsCount = fileSize == Int64(self.chunkSize) ? 1 : (Int(fileSize / Int64(self.chunkSize)) + 1)  //+1: if the file is 5Mb and chunkSize is 10, the 5/10 will be 0 but we have 1 part
        
        //create a part for each chunk
        var chunkIndex = 1
        while partsCount >= chunkIndex
        {
            //reading from file allocates memory in chunk the same size as the chunck
            //need to release it to avoid running out of memory
            autoreleasepool   {
                self.uploadAWSPart(chunkIndex)
                
                chunkIndex += 1
            }
        }
    }
    
    func uploadAWSPart(_ awsPartNumber:Int)
    {
        //Get a presigned URL for AWS S3
        let getPreSignedURLRequest = AWSS3GetPreSignedURLRequest()
        
        //specify which bucket we want to upload to
        getPreSignedURLRequest.bucket = self.bucketName
        
        //specify what is the name of the file
        getPreSignedURLRequest.key = self.fileName
        
        //for upload, we need to do a PUT
        getPreSignedURLRequest.httpMethod = AWSHTTPMethod.PUT;
        
        //this is where the magic happens, you can specify how long you want
        //this pre-signed URL to be valid for, in this case 36 hours
        getPreSignedURLRequest.expires = Date(timeIntervalSinceNow: 36 * 60 * 60);
        
        //Important: set contentType for a PUT request.
        getPreSignedURLRequest.contentType = self.contentType
        
        //Tell AWS which upload you are uploading to, this is a value we got earlier
        getPreSignedURLRequest.setValue(self.multipartUploadId, forRequestParameter: "uploadId")
        
        //tell AWS what is the index of this part, note that this needs to be a string for some reason
        getPreSignedURLRequest.setValue(String(awsPartNumber), forRequestParameter: "partNumber")
        
        //generate the file for the current chunck
        //NSURLSession can only work from files when working in the background
        //so we need to create a file containing just the part required
        let url : URL = self.videoURLs[awsPartNumber - 1]
        
        //AWS wants to get an MD5 hash of the file to make sure everything got transfered ok
        let MD5 = (try? Data(contentsOf: url))?.base64MD5()
        getPreSignedURLRequest.contentMD5 = MD5
        
        //create a presigned URL request for this specific chunk
        let presignedTask = AWSS3PreSignedURLBuilder.default().getPreSignedURL(getPreSignedURLRequest)
        
        //run the request to get a presigned URL
        presignedTask.continueWith(executor: AWSExecutor.mainThread(), block: { (task:AWSTask!) -> AnyObject? in
            if let presignedURL = task.result as URL?
            {
                //we now have the URL we can use to upload this chunk...
                self.startUploadForPresignedURL (presignedURL, chunkURL: url, awsPartNumber: awsPartNumber)
            }
            return nil
        })
    }
    
    func startUploadForPresignedURL (_ presignedURL:URL, chunkURL: URL, awsPartNumber: Int)
    {
        //create the request with the presigned URL
        let URLRequest = NSMutableURLRequest(url: presignedURL)
        URLRequest.cachePolicy = .reloadIgnoringLocalCacheData
        URLRequest.httpMethod = "PUT"
        URLRequest.setValue(self.contentType, forHTTPHeaderField: "Content-Type")
        URLRequest.setValue((try? Data(contentsOf: chunkURL))?.base64MD5(), forHTTPHeaderField: "Content-MD5")
        
        print("presignedURL: \(presignedURL), chunkURL: \(chunkURL), awsPartNumber:\(awsPartNumber)")
        
        //create the upload task with the request
        let uploadTask = self.session!.uploadTask(with: URLRequest as URLRequest, fromFile: chunkURL, completionHandler: { data, response, error in
            if let httpResponse = response as? HTTPURLResponse, httpResponse.statusCode == 200 {
                self.handleSuccessfulPartUploadInSession(partNumber: awsPartNumber, response: httpResponse)
            }
        })
        
        //set the part number as the description so we can keep track of the various tasks
        uploadTask.taskDescription = String(awsPartNumber)
        
        //start the part upload
        uploadTask.resume()
    }
    
    func handleSuccessfulPartUploadInSession (partNumber: Int, response: HTTPURLResponse)
    {
        print(response)
        print("partNumber: \(partNumber)")
        //for each part we need to save the etag and the part number
        guard let completedPart = AWSS3CompletedPart() else { return }
        
        //remember how we saved the part number in the task description, time to get it back
        completedPart.partNumber = NSNumber(integerLiteral: partNumber)
        
        //save the etag as AWS needs that information
        let headers = response.allHeaderFields
        completedPart.eTag = headers["Etag"] as? String
        
        //add the part to the list of completed parts
        self.completedPartsInfo?.parts?.append(completedPart)
        
        //check if there are any other parts uploading
        self.session!.getAllTasks(completionHandler: { (tasks:[URLSessionTask]) -> Void in
            if tasks.count > 0 //completed task are flushed from the list, current task is still listed though, hence 1
            {
                //upload is still progressing
            }
            else
            {
                //all parts were uploaded, let AWS know
                self.completeUpload()
            }
        })
    }
    
    func completeUpload ()
    {
        //For some reason AWS needs the parts sorted, it can't do it on its own...
        let descriptor = NSSortDescriptor(key: "partNumber", ascending: true)
        if let completedPartsInfo = self.completedPartsInfo, let value = (completedPartsInfo.parts as NSArray?) {
            completedPartsInfo.parts = value.sortedArray(using: [descriptor]) as? [AWSS3CompletedPart]
        }
        
        //close up the session as we are done
        self.session!.finishTasksAndInvalidate()
        
        //create the request to complete the multipart upload
        guard let complete = AWSS3CompleteMultipartUploadRequest() else { return }
        complete.uploadId = self.multipartUploadId
        complete.bucket = bucketName
        complete.multipartUpload = completedPartsInfo
        complete.key = accessKey
//        complete.expectedBucketOwner =
        
        //run the request that will complete the uplaod
        let task = AWSS3.default().completeMultipartUpload(complete)
        task.continueWith(block: { task in
            //handle error and do any needed cleanup
            print(task.result)
            print(task.error)
            return nil
        })
    }
}

extension Data {
    func base64MD5() -> String {
        var digest = [UInt8](repeating: 0, count:Int(CC_SHA256_DIGEST_LENGTH))
        
        /// CC_SHA256 performs digest calculation and places the result in the caller-supplied buffer for digest (md)
        /// Takes the strData referenced value (const unsigned char *d) and hashes it into a reference to the digest parameter.
        self.withUnsafeBytes {
            // CommonCrypto
            // extern unsigned char *CC_SHA256(const void *data, CC_LONG len, unsigned char *md)  -|
            // OpenSSL                                                                             |
            // unsigned char *SHA256(const unsigned char *d, size_t n, unsigned char *md)        <-|
            CC_SHA256($0.baseAddress, UInt32(self.count), &digest)
        }
        
        var sha256String = ""
        /// Unpack each byte in the digest array and add them to the sha256String
        for byte in digest {
            sha256String += String(format:"%02x", UInt8(byte))
        }
        
//        print("sha256 hash: \(sha256String)")
        return sha256String
    }
}

extension URL {
    func getFileSize() -> Int {
        let filePath = self.path
        do {
            let attribute = try FileManager.default.attributesOfItem(atPath: filePath)
            if let size = attribute[FileAttributeKey.size] as? NSNumber {
                return size.intValue
            }
        } catch {
            print("Error: \(error)")
        }
        return 0
    }
    
    func getChunkCount(chunckSize: Int) -> Int {
        let fileSize = self.getFileSize()
        return fileSize == chunckSize ? 1 : ((fileSize / chunckSize) + 1)
    }
}
