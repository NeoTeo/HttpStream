//
//  HttpStream.swift
//  
//  HttpStream provides a data stream from an url by parsing the html response 
//  header and chunking and passing on the content only.
//
//  Created by Teo Sartori on 29/09/2016.
//  Copyright © 2016 Matteo Sartori. All rights reserved.
//

import Foundation
//

// This version parses the html response header and the chunking thus
// providing a stream of the data (in this case a tar stream from IPFS).


public class HttpStream : NSObject, URLSessionDataDelegate {
    
    var inputStream: InputStream?
    var outputStream: OutputStream?
    
    var session: URLSession!
    
    var serialDataCacheQ = DispatchQueue(label: "serialDataCacheQ")
    var dataCache = Data()
    
    var currentBufferSize = 256//5120
    
    /// debug vars - remove
    var bytesWrittenToOutputStream = 0
    var totalSessionByteCount: Int?
    var sessionByteCount: Int = 0
    
    public func getReadStream(for url: URL) -> InputStream? {
    
        let config = URLSessionConfiguration.default
        session = URLSession(configuration: config, delegate: self, delegateQueue: nil)
        let task = session.dataTask(with: url)
        
        task.resume()
        
        /// Connect the output stream to the input stream using a given buffer size.
        Stream.getBoundStreams(withBufferSize: currentBufferSize, inputStream: &inputStream, outputStream: &outputStream)
        
        inputStream?.delegate = self
        outputStream?.delegate = self
        
        inputStream?.schedule(in: .main, forMode: .defaultRunLoopMode)
        outputStream?.schedule(in: .main, forMode: .defaultRunLoopMode)
        
        inputStream?.open()
        outputStream?.open()
        
        return inputStream
    }
    
    
    /// Called when url session task completed.
    public func urlSession(_ session: URLSession, task: URLSessionTask, didCompleteWithError error: Error?) {
        if let error = error { print("ERROR urlSession: \(error)") }
        totalSessionByteCount = sessionByteCount
        checkForEnd()
    }
    
    public func urlSession(_ session: URLSession, dataTask: URLSessionDataTask, didReceive data: Data) {

        /// Track the total received bytecount.
        sessionByteCount += data.count
        
        /** We have to access the dataCache through a queue to allow concurrent write access by both
         this method, when it needs to add to the end of the cache, and the stream event handler, when it
         needs to remove data from the front of the cache.
         **/
        serialDataCacheQ.sync {
            dataCache.append(data)
        }
        
        if outputStream?.hasSpaceAvailable == true {
            
            serialDataCacheQ.sync {
                dataCache = fill(stream: outputStream!, with: dataCache)
            }
        }
    }

    public func finish() {
        session.finishTasksAndInvalidate()
    }
}

extension HttpStream : StreamDelegate {
    
    
    public func stream(_ aStream: Stream, handle eventCode: Stream.Event) {
        
        let isInputStream = aStream.isEqual(inputStream)
        
        switch eventCode {
        case Stream.Event.openCompleted:
            break
            
        case Stream.Event.hasSpaceAvailable:
            
            guard isInputStream == false else { break }
            
            serialDataCacheQ.sync {
                /// The returned dataCache is whatever did not fit in the output stream.
                dataCache = fill(stream: outputStream!, with: dataCache)
            }
            
        case Stream.Event.errorOccurred:
            print("Error! \(String(describing: aStream.streamError?.localizedDescription))")
            
        default:
            print("We got nuffink.\(eventCode.rawValue)")
        }
    }
    
    func checkForEnd() {
        if let total = totalSessionByteCount, total == bytesWrittenToOutputStream {
            outputStream?.close()
        }
    }
    
    func fill(stream out: OutputStream, with data: Data) -> Data {
        
        /// Sanity checks
        guard out.hasSpaceAvailable == true else { return data } /// change this to a throw
        var bytesWritten = data.count
        
        if data.count > 0 {
            /// The number of bytes we would like to write to the stream.
            let writeByteCount = min(currentBufferSize, data.count)
            
            /// Turn the data into a byte array
            let bytes = data.withUnsafeBytes { [UInt8](UnsafeBufferPointer(start: $0, count: writeByteCount)) }
            
            /// Attempt to write the bytes to the stream and register how many actually got through.
            bytesWritten = out.write(bytes, maxLength: writeByteCount)
            
            guard bytesWritten != -1 else {
                print("Error writing: \(String(describing: out.streamError))")
                return data
            }
            /// Debug: Track total bytes written out.
            bytesWrittenToOutputStream += bytesWritten
        }
        checkForEnd()
        return data.subdata(in: bytesWritten ..< data.count)
    }
}
