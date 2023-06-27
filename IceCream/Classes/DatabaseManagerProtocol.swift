//
//  DatabaseManager.swift
//  IceCream
//
//  Created by caiyue on 2019/4/22.
//

import CloudKit

protocol DatabaseManager: AnyObject {
    
    /// A conduit for accessing and performing operations on the data of an app container.
    var database: CKDatabase { get }
    
    /// An encapsulation of content associated with an app.
    var container: CKContainer { get }
    
    var syncObjects: [Syncable] { get }
    
    init(objects: [Syncable], container: CKContainer)
    
    func prepare()
    
    func fetchChangesInDatabase(_ callback: ((Error?) -> Void)?)
    
    /// The CloudKit Best Practice is out of date, now use this:
    /// https://developer.apple.com/documentation/cloudkit/ckoperation
    /// Which problem does this func solve? E.g.:
    /// 1.(Offline) You make a local change, involve a operation
    /// 2. App exits or ejected by user
    /// 3. Back to app again
    /// The operation resumes! All works like a magic!
    func resumeLongLivedOperationIfPossible()
    
    func createCustomZonesIfAllowed()
    func startObservingRemoteChanges()
    func startObservingTermination()
    func createDatabaseSubscriptionIfHaveNot()
    func registerLocalDatabase()
    
    func cleanUp()
}

extension DatabaseManager {
    
    func prepare() {
        syncObjects.forEach {
            /// 注册将本地数据同步到CloudKit的数据通道
            $0.pipeToEngine = { [weak self] recordsToStore, recordIDsToDelete in
                guard let self = self else { return }
                self.syncRecordsToCloudKit(recordsToStore: recordsToStore, recordIDsToDelete: recordIDsToDelete)
            }
        }
    }
    
    func resumeLongLivedOperationIfPossible() {
        container.fetchAllLongLivedOperationIDs { [weak self]( opeIDs, error) in
            guard let self = self, error == nil, let ids = opeIDs else { return }
            for id in ids {
                self.container.fetchLongLivedOperation(withID: id, completionHandler: { [weak self](ope, error) in
                    guard let self = self, error == nil else { return }
                    if let modifyOp = ope as? CKModifyRecordsOperation {
                        modifyOp.modifyRecordsCompletionBlock = { (_,_,_) in
                            print("Resume modify records success!")
                        }
                        // The Apple's example code in doc(https://developer.apple.com/documentation/cloudkit/ckoperation/#1666033)
                        // tells we add operation in container. But however it crashes on iOS 15 beta versions.
                        // And the crash log tells us to "CKDatabaseOperations must be submitted to a CKDatabase".
                        // So I guess there must be something changed in the daemon. We temperorily add this availabilty check.
                        if #available(iOS 15, *) {
                            self.database.add(modifyOp)
                        } else {
                            self.container.add(modifyOp)
                        }
                    }
                })
            }
        }
    }
    
    func startObservingRemoteChanges() {
        NotificationCenter.default.addObserver(forName: Notifications.cloudKitDataDidChangeRemotely.name, object: nil, queue: nil, using: { [weak self](_) in
            guard let self = self else { return }
            DispatchQueue.global(qos: .utility).async {
                SyncEnginLogHandler.log(tag: .RemoteChange, msg: "=============================== 收到远程数据变更通知 =======================")
                self.fetchChangesInDatabase { error in
                    guard error == nil else { return }
                    NotificationCenter.default.post(name: Notifications.didReceiveRemoteChange.name, object: nil)
                }
            }
        })
    }
    
    /// Sync local data to CloudKit
    /// For more about the savePolicy: https://developer.apple.com/documentation/cloudkit/ckrecordsavepolicy
    public func syncRecordsToCloudKit(recordsToStore: [CKRecord], recordIDsToDelete: [CKRecord.ID], completion: ((Error?) -> ())? = nil) {
        
        SyncEnginLogHandler.log(tag: .PushTags, msg: "=============================== 将数据同步到CloudKit =============================== ")
        
        let modifyOpe = CKModifyRecordsOperation(recordsToSave: recordsToStore, recordIDsToDelete: recordIDsToDelete)
        
        SyncEnginLogHandler.log(tag: .PushTags, msg: "=============== recordsToStore 要存储到云端的数据 [Begin] ====================")
        for store in recordsToStore {
            SyncEnginLogHandler.log(tag: .PushTags, msg: " recordType = \(store.recordType) store = \(store)")
        }
        SyncEnginLogHandler.log(tag: .PushTags, msg: "=============== recordsToStore 要存储到云端的数据 [End] ====================")
        
        
        SyncEnginLogHandler.log(tag: .PushTags, msg: "=============== recordIDsToDelete 要从云端删除的数据 [Begin] ====================")
        for delete in recordIDsToDelete {
            SyncEnginLogHandler.log(tag: .PushTags, msg: " delete = \(delete.recordName)")
        }
        SyncEnginLogHandler.log(tag: .PushTags, msg: "=============== recordIDsToDelete 要从云端删除的数据 [End] ====================")
        
        
        if #available(iOS 11.0, OSX 10.13, tvOS 11.0, watchOS 4.0, *) {
            let config = CKOperation.Configuration()
            config.isLongLived = true
            modifyOpe.configuration = config
        } else {
            // Fallback on earlier versions
            modifyOpe.isLongLived = true
        }
        
        // We use .changedKeys savePolicy to do unlocked changes here cause my app is contentious and off-line first
        // Apple suggests using .ifServerRecordUnchanged save policy
        // For more, see Advanced CloudKit(https://developer.apple.com/videos/play/wwdc2014/231/)
        modifyOpe.savePolicy = .changedKeys
        
        // To avoid CKError.partialFailure, make the operation atomic (if one record fails to get modified, they all fail)
        // If you want to handle partial failures, set .isAtomic to false and implement CKOperationResultType .fail(reason: .partialFailure) where appropriate
        modifyOpe.isAtomic = true
        
        modifyOpe.modifyRecordsCompletionBlock = {
            [weak self]
            (_, _, error) in
            
            guard let self = self else { return }
            SyncEnginLogHandler.log(tag: .PushTags, msg: "modifyRecordsCompletionBlock error \(String(describing: error))")
            switch ErrorHandler.shared.resultType(with: error) {
            case .success:
                SyncEnginLogHandler.log(tag: .PushTags, msg: "modifyRecordsCompletionBlock success")
                DispatchQueue.main.async {
                    completion?(nil)
                }
            case .retry(let timeToWait, _):
                SyncEnginLogHandler.log(tag: .PushTags, msg: "modifyRecordsCompletionBlock retry timeToWait = \(timeToWait)")
                ErrorHandler.shared.retryOperationIfPossible(retryAfter: timeToWait) {
                    self.syncRecordsToCloudKit(recordsToStore: recordsToStore, recordIDsToDelete: recordIDsToDelete, completion: completion)
                }
            case .chunk:
                SyncEnginLogHandler.log(tag: .PushTags, msg: "modifyRecordsCompletionBlock chunk")
                /// CloudKit says maximum number of items in a single request is 400.
                /// So I think 300 should be fine by them.
                let chunkedRecords = recordsToStore.chunkItUp(by: 300)
                for chunk in chunkedRecords {
                    self.syncRecordsToCloudKit(recordsToStore: chunk, recordIDsToDelete: recordIDsToDelete, completion: completion)
                }
            default:
                return
            }
        }
        
        database.add(modifyOpe)
    }
    
}
