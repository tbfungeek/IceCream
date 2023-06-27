//
//  PublicDatabaseManager.swift
//  IceCream
//
//  Created by caiyue on 2019/4/22.
//

#if os(macOS)
import Cocoa
#else
import UIKit
#endif

import CloudKit

final class PublicDatabaseManager: DatabaseManager {
    
    let container: CKContainer
    let database: CKDatabase
    
    let syncObjects: [Syncable]
    
    init(objects: [Syncable], container: CKContainer) {
        self.syncObjects = objects
        self.container = container
        self.database = container.publicCloudDatabase
    }
    
    func fetchChangesInDatabase(_ callback: ((Error?) -> Void)?) {
        SyncEnginLogHandler.log(tag: .FetchTags, msg: "[fetchChangesInDatabase] 准备拉取iCloud数据到本地")
        syncObjects.forEach { [weak self] syncObject in
            let predicate = NSPredicate(value: true)
            let query = CKQuery(recordType: syncObject.recordType, predicate: predicate)
            let queryOperation = CKQueryOperation(query: query)
            self?.excuteQueryOperation(queryOperation: queryOperation, on: syncObject, callback: {
                syncObject.resolvePendingRelationships()
                callback?($0)
            })
        }
    }
    
    func createCustomZonesIfAllowed() {
        
    }
    
    func createDatabaseSubscriptionIfHaveNot() {
        syncObjects.forEach { createSubscriptionInPublicDatabase(on: $0) }
    }
    
    func startObservingTermination() {
        #if os(iOS) || os(tvOS)
        
        NotificationCenter.default.addObserver(self, selector: #selector(self.cleanUp), name: UIApplication.willTerminateNotification, object: nil)
        
        #elseif os(macOS)
        
        NotificationCenter.default.addObserver(self, selector: #selector(self.cleanUp), name: NSApplication.willTerminateNotification, object: nil)
        
        #endif
    }
    
    func registerLocalDatabase() {
        syncObjects.forEach { object in
            DispatchQueue.main.async {
                object.registerLocalDatabase()
            }
        }
    }
    
    // MARK: - Private Methods
    private func excuteQueryOperation(queryOperation: CKQueryOperation,on syncObject: Syncable, callback: ((Error?) -> Void)? = nil) {
        
        SyncEnginLogHandler.log(tag: .FetchTags, msg: "[excuteQueryOperation] syncObject = \(syncObject.recordType)")
        
        queryOperation.recordFetchedBlock = { record in
            SyncEnginLogHandler.log(tag: .FetchTags, msg: "[excuteQueryOperation] recordFetchedBlock = recordType = \(record.recordType)")
            syncObject.add(record: record)
        }
        
        queryOperation.queryCompletionBlock = { [weak self] cursor, error in
            guard let self = self else { return }
            SyncEnginLogHandler.log(tag: .FetchTags, msg: "[excuteQueryOperation] queryCompletionBlock = recordType = \(syncObject.recordType) error = \(String(describing: error))")
            if let cursor = cursor {
                let subsequentQueryOperation = CKQueryOperation(cursor: cursor)
                self.excuteQueryOperation(queryOperation: subsequentQueryOperation, on: syncObject, callback: callback)
                return
            }
            switch ErrorHandler.shared.resultType(with: error) {
            case .success:
                SyncEnginLogHandler.log(tag: .FetchTags, msg: "[excuteQueryOperation] success")
                DispatchQueue.main.async {
                    callback?(nil)
                }
            case .retry(let timeToWait, _):
                SyncEnginLogHandler.log(tag: .FetchTags, msg: "[excuteQueryOperation] retry timeToWait = \(timeToWait)")
                ErrorHandler.shared.retryOperationIfPossible(retryAfter: timeToWait, block: {
                    self.excuteQueryOperation(queryOperation: queryOperation, on: syncObject, callback: callback)
                })
            default:
                break
            }
        }
        
        database.add(queryOperation)
    }
    
    private func createSubscriptionInPublicDatabase(on syncObject: Syncable) {
       #if os(iOS) || os(tvOS) || os(macOS)
       let predict = NSPredicate(value: true)
       let subscription = CKQuerySubscription(recordType: syncObject.recordType, predicate: predict, subscriptionID: IceCreamSubscription.cloudKitPublicDatabaseSubscriptionID.id, options: [CKQuerySubscription.Options.firesOnRecordCreation, CKQuerySubscription.Options.firesOnRecordUpdate, CKQuerySubscription.Options.firesOnRecordDeletion])

       let notificationInfo = CKSubscription.NotificationInfo()
       notificationInfo.shouldSendContentAvailable = true // Silent Push

       subscription.notificationInfo = notificationInfo

       let createOp = CKModifySubscriptionsOperation(subscriptionsToSave: [subscription], subscriptionIDsToDelete: [])
       createOp.modifySubscriptionsCompletionBlock = { _, _, _ in
           SyncEnginLogHandler.log(tag: .RemoteChange, msg: "====================== createSubscriptionInPublicDatabase modifySubscriptionsCompletionBlock ====================")
       }
       createOp.qualityOfService = .utility
       database.add(createOp)
       #endif
    }
    
    @objc func cleanUp() {
        for syncObject in syncObjects {
            syncObject.cleanUp()
        }
    }
}
