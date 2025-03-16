//
//  RetentionPolicy.swift
//  swift-jobs-postgres
//
//  Created by Stevenson Michel on 3/15/25.
//

public struct RetentionPolicy: Sendable {
    
    public struct RetainData: Sendable {
        /// Duration in ISO 8601 format (e.g., "P30D" for 30 days)
        public var duration: String
        
        public init(duration: String) {
            self.duration = duration
        }
    }
    
//    public var days: Int
//    
//    public init(days: Int) {
//        self.days = days
//    }
    /// Jobs with status cancelled
    public var canceled: RetainData
    /// Jobs with status completed
    public var completed: RetainData
    /// Jobs with status failed
    public var failed: RetainData
    
    public init(canceled: RetainData, completed: RetainData, failed: RetainData) {
        self.canceled = canceled
        self.completed = completed
        self.failed = failed
    }
}
