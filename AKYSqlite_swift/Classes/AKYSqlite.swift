//
//  AKYSqlite.swift
//  AKYSqlite
//
//  Created by Aikyuichi on 10/9/19.
//  Copyright (c) 2022 aikyuichi <aikyu.sama@gmail.com>
//

import Foundation

public class AKYSqlite {
    
    static let AKYSQLITE_DB_PATHS = "AKYSqlite_db_paths"
    
    private init() {}
    
    static public func registerDatabasePath(path: String, forKey key: String) {
        var dbPaths = UserDefaults.standard.dictionary(forKey: AKYSQLITE_DB_PATHS) ?? [:]
        dbPaths[key] = path
        UserDefaults.standard.setValue(dbPaths, forKey: AKYSQLITE_DB_PATHS)
    }
    
    static public func registerDatabase(name: String, fromMainBundleForKey key: String) {
        let url = NSURL(fileURLWithPath: name)
        let dbPath = Bundle.main.path(forResource: url.deletingPathExtension?.lastPathComponent, ofType: url.pathExtension)!
        self.registerDatabasePath(path: dbPath, forKey: key)
    }
    
    static public func registerDatabase(name: String, fromDocumentDirectoryForKey key: String) {
        if let documentPath = NSSearchPathForDirectoriesInDomains(.documentDirectory, .userDomainMask, true).first {
            let dbPath = (documentPath as NSString).appendingPathComponent(name)
            self.registerDatabasePath(path: dbPath, forKey: key)
        }
    }
    
    static public func registerDatabase(name: String, copyFromMainBundleToDocumentDirectoryForKey key: String) {
        let url = NSURL(fileURLWithPath: name)
        let dbPathFrom = Bundle.main.path(forResource: url.deletingPathExtension?.lastPathComponent, ofType: url.pathExtension)!
        if let documentPath = NSSearchPathForDirectoriesInDomains(.documentDirectory, .userDomainMask, true).first {
            let dbPathTo = (documentPath as NSString).appendingPathComponent(name)
            if !FileManager.default.fileExists(atPath: dbPathTo) {
                try! FileManager.default.copyItem(atPath: dbPathFrom, toPath: dbPathTo)
            }
            self.registerDatabasePath(path: dbPathTo, forKey: key)
        }
    }
    
    static public func unregisterDatabase(forkey key: String) {
        var dbPaths = UserDefaults.standard.dictionary(forKey: AKYSQLITE_DB_PATHS)
        dbPaths?.removeValue(forKey: key)
        UserDefaults.standard.setValue(dbPaths, forKey: AKYSQLITE_DB_PATHS)
    }
    
    static public func databasePath(forKey key: String) -> String? {
        let dbPaths = UserDefaults.standard.dictionary(forKey: AKYSQLITE_DB_PATHS)
        return dbPaths?[key] as? String;
    }
    
    static public func runUpdater(forKey key: String) {
        let updates = self.getUpdates(forKey: key)
        if updates.isEmpty {
            return
        }
        for update in updates {
            if !self.executeUpdate(update) {
                if update.errorLevel == AKYUpdaterErrorLevel.skip {
                    print("update failed but skipped: \(update)")
                } else {
                    print("update failed: \(update)")
                    break
                }
            }
        }
    }
    
    static private func getUpdates(forKey key: String) -> [UpdateItem] {
        var items: [UpdateItem] = []
        let db = AKYDatabase(forKey: key)
        if db.open() {
            let stmt = db.prepareStatement(query: "SELECT id, db_key, db_version, sql, error_level FROM updater")
            while stmt.step() {
                items.append(UpdateItem(
                    id: stmt.getInt(forName: "id")!,
                    dbKey: stmt.getString(forName: "db_key")!,
                    dbVersion: stmt.getInt(forName: "db_version")!,
                    sqlCommands: stmt.getString(forName: "sql")!,
                    errorLevel: AKYUpdaterErrorLevel(rawValue: stmt.getInt(forName: "error_level")!) ?? .log
                ))
            }
            stmt.finalize()
            db.close()
        }
        return items
    }
    
    static private func executeUpdate(_ update: UpdateItem) -> Bool {
        var result = false
        if self.databaseExists(forKey: update.dbKey) {
            let db = AKYDatabase(forKey: update.dbKey)
            if db.openTransaction() {
                if db.userVersion <= update.dbVersion {
                    for command in update.sqlCommands {
                        let stmt = db.prepareStatement(query: String(command))
                        _ = stmt.step()
                        stmt.finalize()
                        result = !stmt.failed
                        if update.errorLevel.rawValue > AKYUpdaterErrorLevel.skip.rawValue && !result {
                            break
                        }
                    }
                } else {
                    result = true
                }
                db.close()
            }
        }
        return result
    }
    
    static private func databaseExists(forKey key: String) -> Bool {
        if let dbPaths = UserDefaults.standard.dictionary(forKey: AKYSQLITE_DB_PATHS) as? [String: String], let dbPath = dbPaths[key] {
            return FileManager.default.fileExists(atPath: dbPath)
        }
        return false
    }
}

private struct UpdateItem {
    let id: Int
    let dbKey: String
    let dbVersion: Int
    let sqlCommands: [String]
    let errorLevel: AKYUpdaterErrorLevel
    
    init(id: Int, dbKey: String, dbVersion: Int, sqlCommands: String, errorLevel: AKYUpdaterErrorLevel) {
        self.id = id
        self.dbKey = dbKey
        self.dbVersion = dbVersion
        self.sqlCommands = sqlCommands.split(separator: ";", omittingEmptySubsequences: true).map { String($0) }
        self.errorLevel = errorLevel
    }
}

private enum AKYUpdaterErrorLevel: Int {
    case skip = 0
    case log = 1
}
