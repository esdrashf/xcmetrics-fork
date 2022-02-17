// Copyright (c) 2020 Spotify AB.
//
// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

import Foundation
import Vapor
import SotoS3
import SotoSTS

/// `LogFileRepository` that uses Amazon S3 to store and fetch logs
struct LogFileS3Repository: LogFileRepository {

    let bucketName: String
    let s3: S3

    init(config: Configuration) {
        guard let bucketName = config.s3Bucket else {
            preconditionFailure("No S3 bucket name provided (XCMETRICS_S3_BUCKET)")
        }
        guard let regionName = config.s3Region,
              let region = SotoCore.Region(awsRegionName: regionName) else {
                preconditionFailure("Invalid S3 region provided (XCMETRICS_S3_REGION)")
        }
        guard let credentialProvider = Self.credentialProviderFactory(from: config, region: region) else {
            preconditionFailure("Invalid S3 credentials provided, you have to provide either (AWS_IAM_ROLE) or (AWS_ACCESS_KEY_ID + AWS_SECRET_ACCESS_KEY_ID)")
        }

        let client = AWSClient(credentialProvider: credentialProvider, httpClientProvider: .createNew)

        self.bucketName = bucketName
        self.s3 = S3(client: client, region: region)
    }

    private static func credentialProviderFactory(from config: Configuration,
                                                  region: SotoCore.Region) -> CredentialProviderFactory? {

        if let iamRole = config.awsIamRole {
            let request = STS.AssumeRoleRequest(roleArn: iamRole,
                                                roleSessionName: "session-name")

            return .stsAssumeRole(request: request,
                                  credentialProvider: .ec2,
                                  region: region)

        } else if let accessKey = config.awsAccessKeyId,
                  let secretKey = config.awsSecretAccessKey {

            return .static(accessKeyId: accessKey,
                           secretAccessKey: secretKey)
        }

        return nil
    }

    func put(logFile: File) throws -> URL {
        let data = Data(logFile.data.xcm_onlyFileData().readableBytesView)
        let payload = AWSPayload.data(data)
        let putObjectRequest = S3.PutObjectRequest(acl: .private,
                                                   body: payload,
                                                   bucket: bucketName,
                                                   contentLength: Int64(data.count),
                                                   key: logFile.filename)
        let fileURL = try s3.putObject(putObjectRequest)
            .map { _ -> URL? in
                return URL(string: "s3://\(bucketName)/\(logFile.filename)")
            }.wait()
        guard let url = fileURL else {
            throw RepositoryError.unexpected(message: "Invalid url of \(logFile.filename)")
        }
        return url
    }

    func get(logURL: URL) throws -> LogFile {
        guard let bucket = logURL.host else {
            throw RepositoryError.unexpected(message: "URL is not an S3 url \(logURL)")
        }
        let fileName = logURL.lastPathComponent
        let request = S3.GetObjectRequest(bucket: bucket, key: fileName)
        let response = try s3.getObject(request).wait()

        guard let data = response.body?.asData() else {
            throw RepositoryError.unexpected(message: "There was an error downloading file \(logURL)")
        }
        let tmp = try TemporaryFile(creatingTempDirectoryForFilename: "\(UUID().uuidString).xcactivitylog")
        try data.write(to: tmp.fileURL)
        return LogFile(remoteURL: logURL, localURL: tmp.fileURL)
    }

}
