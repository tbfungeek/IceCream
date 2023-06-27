//
//  LogTags.swift
//  IceCream
//
//  Created by 林晓海 on 2023/6/27.
//

import Foundation

public protocol LogProtocol {
    func LogMsg(tag:String,msg:String)
}

public class SyncEnginLogHandler {
    
    private static var logProxy:(any LogProtocol)? = nil
    
    public static func setUpProxy(logProxy:(any LogProtocol)) {
        self.logProxy = logProxy
    }
    
    static func log(tag:SyncLogTags,msg:String) {
        guard let logProxy = logProxy else { return }
        logProxy.LogMsg(tag: tag.rawValue, msg: msg)
    }
    
}

enum SyncLogTags:String {
    case SyncEngine
    case FetchTags
    case PushTags
    case RemoteChange
}
