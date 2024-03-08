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
    
    static public func registerDatabase(path: String, forKey key: String) {
        var dbPaths = UserDefaults.standard.dictionary(forKey: AKYSQLITE_DB_PATHS) ?? [:]
        dbPaths[key] = path
        UserDefaults.standard.setValue(dbPaths, forKey: AKYSQLITE_DB_PATHS)
    }
    
    static public func registerDatabase(name: String, fromMainBundleForKey key: String, copyToDocumentDirectory copy: Bool = false) {
        let url = NSURL(fileURLWithPath: name)
        let dbPath = Bundle.main.path(forResource: url.deletingPathExtension?.lastPathComponent, ofType: url.pathExtension)!
        if copy {
            if let documentPath = NSSearchPathForDirectoriesInDomains(.documentDirectory, .userDomainMask, true).first {
                let dbPathTo = (documentPath as NSString).appendingPathComponent(name)
                if !FileManager.default.fileExists(atPath: dbPathTo) {
                    try! FileManager.default.copyItem(atPath: dbPath, toPath: dbPathTo)
                }
                self.registerDatabase(path: dbPathTo, forKey: key)
            }
        } else {
            self.registerDatabase(path: dbPath, forKey: key)
        }
    }
    
    static public func registerDatabase(name: String, fromDocumentDirectoryForKey key: String) {
        if let documentPath = NSSearchPathForDirectoriesInDomains(.documentDirectory, .userDomainMask, true).first {
            let dbPath = (documentPath as NSString).appendingPathComponent(name)
            self.registerDatabase(path: dbPath, forKey: key)
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
    
    static public func runUpdater(path: String? = nil) {
        if let path = path ?? Bundle.main.path(forResource: "updater", ofType: "json") {
            let updates = self.getUpdates(filename: path)
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
        } else {
            print("updater.json not found in main bundle")
        }
    }
    
    static private func getUpdates(filename: String) -> [UpdateItem] {
        var items: [UpdateItem] = []
        do {
            if let data = try String(contentsOfFile: filename).data(using: .utf8),
               let json = try JSONSerialization.jsonObject(with: data) as? [[String: Any]] {
                for item in json {
                    if let updateItem = UpdateItem(json: item) {
                        items.append(updateItem)
                    } else {
                        break
                    }
                }
            }
        } catch {
            print(error)
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
    let errorLevel: AKYUpdaterErrorLevel
    let sqlCommands: [String]
    
    init?(json: [String: Any]) {
        guard let id = json["id"] as? Int,
              let dbKey = json["dbKey"] as? String,
              let dbVersion = json["dbVersion"] as? Int,
              let errorLevelRawValue = json["errorLevel"] as? Int,
              let errorLevel = AKYUpdaterErrorLevel(rawValue: errorLevelRawValue),
              let sqlCommands = json["sqlCommands"] as? [String]
        else {
            print("updater: invalid format")
            return nil
        }
        self.id = id
        self.dbKey = dbKey
        self.dbVersion = dbVersion
        self.errorLevel = errorLevel
        self.sqlCommands = sqlCommands
    }
}

private enum AKYUpdaterErrorLevel: Int {
    case skip = 0
    case fail = 1
}
