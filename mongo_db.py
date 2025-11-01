import os
import logging
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure, OperationFailure
from pymongo.server_api import ServerApi
from datetime import datetime
import asyncio
import base64

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("MongoDB")

class MongoDBManager:
    def __init__(self):
        # Get MongoDB Atlas connection string from environment
        self.uri = os.environ.get("MONGODB_URI")
        if not self.uri:
            log.error("❌ MONGODB_URI environment variable not set!")
            # Fallback for local testing
            self.uri = "mongodb://localhost:27017/"
            
        self.db_name = os.environ.get("MONGODB_DB_NAME", "aura_farming_bot")
        self.client = None
        self.db = None
        self.connect()
    
    def connect(self):
        """Connect to MongoDB Atlas"""
        try:
            # For MongoDB Atlas with your connection string
            self.client = MongoClient(
                self.uri,
                server_api=ServerApi('1'),
                retryWrites=True,
                w='majority',
                connectTimeoutMS=30000,
                socketTimeoutMS=30000,
                serverSelectionTimeoutMS=30000
            )
            self.db = self.client[self.db_name]
            
            # Test connection with ping
            self.client.admin.command('ping')
            log.info("✅ Connected to MongoDB Atlas successfully!")
            log.info(f"📊 Database: {self.db_name}")
            
            # Create indexes for better performance
            self.create_indexes()
            
        except ConnectionFailure as e:
            log.error(f"❌ Failed to connect to MongoDB Atlas: {e}")
            raise
        except Exception as e:
            log.error(f"❌ MongoDB Atlas connection error: {e}")
            raise
    
    def create_indexes(self):
        """Create necessary indexes for optimal performance"""
        try:
            # Index for approved_users collection
            self.db.approved_users.create_index("user_id", unique=True)
            self.db.approved_users.create_index("expiration")
            
            # Index for user_config collection
            self.db.user_config.create_index("user_id", unique=True)
            
            # Index for user_data collection
            self.db.user_data.create_index("user_id", unique=True)
            
            # Index for sessions collection with TTL (auto-delete after 24 hours)
            self.db.sessions.create_index("last_accessed", expireAfterSeconds=86400)
            
            # Index for session_files collection
            self.db.session_files.create_index("user_id", unique=True)
            
            # Index for admin data in user_data collection
            self.db.user_data.create_index("type")
            
            log.info("✅ MongoDB Atlas indexes created successfully")
            
        except Exception as e:
            log.error(f"❌ Error creating indexes: {e}")

    # ==============================
    # ADMIN MANAGEMENT (NEW)
    # ==============================
    
    def save_admins(self, admin_ids):
        """Save admin list to MongoDB"""
        try:
            admin_data = {
                "type": "admin_list",
                "admin_ids": list(admin_ids),
                "last_updated": datetime.utcnow()
            }
            
            result = self.db.user_data.update_one(
                {"type": "admin_list"},
                {"$set": admin_data},
                upsert=True
            )
            
            log.info(f"✅ Saved {len(admin_ids)} admins to MongoDB")
            return True
            
        except Exception as e:
            log.error(f"❌ Error saving admins: {e}")
            return False
    
    def get_admins(self):
        """Get admin list from MongoDB"""
        try:
            admin_data = self.db.user_data.find_one({"type": "admin_list"})
            if admin_data and "admin_ids" in admin_data:
                return set(admin_data["admin_ids"])
            return set()
            
        except Exception as e:
            log.error(f"❌ Error loading admins: {e}")
            return set()

    # ==============================
    # SESSION FILE MANAGEMENT (NEW)
    # ==============================
    
    def save_session_file(self, user_id, session_data):
        """Save session file data to MongoDB as base64 encoded string"""
        try:
            # Encode session data to base64 for safe storage
            encoded_data = base64.b64encode(session_data).decode('utf-8')
            
            session_file_doc = {
                "user_id": user_id,
                "session_data": encoded_data,
                "created_at": datetime.utcnow(),
                "last_accessed": datetime.utcnow()
            }
            
            result = self.db.session_files.update_one(
                {"user_id": user_id},
                {"$set": session_file_doc},
                upsert=True
            )
            
            log.info(f"✅ Session file for user {user_id} saved to MongoDB")
            return True
            
        except Exception as e:
            log.error(f"❌ Error saving session file for user {user_id}: {e}")
            return False
    
    def get_session_file(self, user_id):
        """Get session file data from MongoDB and decode from base64"""
        try:
            session_doc = self.db.session_files.find_one({"user_id": user_id})
            if session_doc and "session_data" in session_doc:
                # Update last accessed time
                self.db.session_files.update_one(
                    {"user_id": user_id},
                    {"$set": {"last_accessed": datetime.utcnow()}}
                )
                
                # Decode base64 data back to bytes
                decoded_data = base64.b64decode(session_doc["session_data"])
                return decoded_data
            return None
            
        except Exception as e:
            log.error(f"❌ Error getting session file for user {user_id}: {e}")
            return None
    
    def delete_session_file(self, user_id):
        """Delete session file from MongoDB"""
        try:
            result = self.db.session_files.delete_one({"user_id": user_id})
            if result.deleted_count > 0:
                log.info(f"✅ Session file for user {user_id} deleted from MongoDB")
            return result.deleted_count > 0
            
        except Exception as e:
            log.error(f"❌ Error deleting session file for user {user_id}: {e}")
            return False
    
    def session_file_exists(self, user_id):
        """Check if session file exists in MongoDB"""
        try:
            count = self.db.session_files.count_documents({"user_id": user_id})
            return count > 0
        except Exception as e:
            log.error(f"❌ Error checking session file existence for user {user_id}: {e}")
            return False

    # ==============================
    # APPROVED USERS MANAGEMENT
    # ==============================
    
    def save_approved_user(self, user_id, expiration=None):
        """Save approved user to MongoDB"""
        try:
            user_data = {
                "user_id": user_id,
                "expiration": expiration,
                "approved_at": datetime.utcnow(),
                "last_updated": datetime.utcnow()
            }
            
            result = self.db.approved_users.update_one(
                {"user_id": user_id},
                {"$set": user_data},
                upsert=True
            )
            
            log.info(f"✅ Approved user {user_id} saved to MongoDB")
            return True
            
        except Exception as e:
            log.error(f"❌ Error saving approved user {user_id}: {e}")
            return False
    
    def remove_approved_user(self, user_id):
        """Remove approved user from MongoDB"""
        try:
            result = self.db.approved_users.delete_one({"user_id": user_id})
            if result.deleted_count > 0:
                log.info(f"✅ Approved user {user_id} removed from MongoDB")
            return result.deleted_count > 0
            
        except Exception as e:
            log.error(f"❌ Error removing approved user {user_id}: {e}")
            return False
    
    def get_approved_users(self):
        """Get all approved users from MongoDB"""
        try:
            users = list(self.db.approved_users.find({}))
            approved_dict = {}
            
            for user in users:
                approved_dict[user["user_id"]] = user["expiration"]
            
            log.info(f"✅ Loaded {len(approved_dict)} approved users from MongoDB")
            return approved_dict
            
        except Exception as e:
            log.error(f"❌ Error loading approved users: {e}")
            return {}
    
    def cleanup_expired_approvals(self):
        """Remove expired approvals from MongoDB"""
        try:
            current_time = datetime.utcnow().timestamp()
            result = self.db.approved_users.delete_many({
                "expiration": {"$ne": None},
                "expiration": {"$lt": current_time}
            })
            
            if result.deleted_count > 0:
                log.info(f"✅ Removed {result.deleted_count} expired approvals from MongoDB")
            
            return result.deleted_count
            
        except Exception as e:
            log.error(f"❌ Error cleaning up expired approvals: {e}")
            return 0
    
    # ==============================
    # USER CONFIG MANAGEMENT
    # ==============================
    
    def save_user_config(self, user_id, config_data):
        """Save user configuration to MongoDB"""
        try:
            user_config = {
                "user_id": user_id,
                "config": config_data,
                "last_updated": datetime.utcnow()
            }
            
            result = self.db.user_config.update_one(
                {"user_id": user_id},
                {"$set": user_config},
                upsert=True
            )
            
            log.info(f"✅ User config for {user_id} saved to MongoDB")
            return True
            
        except Exception as e:
            log.error(f"❌ Error saving user config {user_id}: {e}")
            return False
    
    def get_user_config(self, user_id):
        """Get user configuration from MongoDB"""
        try:
            config_doc = self.db.user_config.find_one({"user_id": user_id})
            if config_doc and "config" in config_doc:
                return config_doc["config"]
            return {}
            
        except Exception as e:
            log.error(f"❌ Error getting user config {user_id}: {e}")
            return {}
    
    def get_all_user_configs(self):
        """Get all user configurations from MongoDB"""
        try:
            configs = list(self.db.user_config.find({}))
            config_dict = {}
            
            for config in configs:
                config_dict[config["user_id"]] = config.get("config", {})
            
            return config_dict
            
        except Exception as e:
            log.error(f"❌ Error loading all user configs: {e}")
            return {}
    
    # ==============================
    # USER DATA MANAGEMENT
    # ==============================
    
    def save_user_data(self, user_id, user_data):
        """Save user data to MongoDB"""
        try:
            data_doc = {
                "user_id": user_id,
                "data": user_data,
                "last_updated": datetime.utcnow()
            }
            
            result = self.db.user_data.update_one(
                {"user_id": user_id},
                {"$set": data_doc},
                upsert=True
            )
            
            log.info(f"✅ User data for {user_id} saved to MongoDB")
            return True
            
        except Exception as e:
            log.error(f"❌ Error saving user data {user_id}: {e}")
            return False
    
    def get_user_data(self, user_id):
        """Get user data from MongoDB"""
        try:
            data_doc = self.db.user_data.find_one({"user_id": user_id})
            if data_doc and "data" in data_doc:
                return data_doc["data"]
            return {"gc_noti": False, "group_id": None}
            
        except Exception as e:
            log.error(f"❌ Error getting user data {user_id}: {e}")
            return {"gc_noti": False, "group_id": None}
    
    def get_all_user_data(self):
        """Get all user data from MongoDB"""
        try:
            all_data = list(self.db.user_data.find({}))
            data_dict = {}
            
            for data in all_data:
                data_dict[data["user_id"]] = data.get("data", {})
            
            return data_dict
            
        except Exception as e:
            log.error(f"❌ Error loading all user data: {e}")
            return {}
    
    # ==============================
    # SESSION MANAGEMENT
    # ==============================
    
    def save_session_state(self, user_id, session_data):
        """Save session state to MongoDB"""
        try:
            session_doc = {
                "user_id": user_id,
                "session_data": session_data,
                "last_accessed": datetime.utcnow()
            }
            
            result = self.db.sessions.update_one(
                {"user_id": user_id},
                {"$set": session_doc},
                upsert=True
            )
            
            log.debug(f"✅ Session state for {user_id} saved to MongoDB")
            return True
            
        except Exception as e:
            log.error(f"❌ Error saving session state {user_id}: {e}")
            return False
    
    def get_session_state(self, user_id):
        """Get session state from MongoDB"""
        try:
            session_doc = self.db.sessions.find_one({"user_id": user_id})
            if session_doc and "session_data" in session_doc:
                return session_doc["session_data"]
            return {}
            
        except Exception as e:
            log.error(f"❌ Error getting session state {user_id}: {e}")
            return {}
    
    def delete_session_state(self, user_id):
        """Delete session state from MongoDB"""
        try:
            result = self.db.sessions.delete_one({"user_id": user_id})
            if result.deleted_count > 0:
                log.info(f"✅ Session state for {user_id} deleted from MongoDB")
            return result.deleted_count > 0
            
        except Exception as e:
            log.error(f"❌ Error deleting session state {user_id}: {e}")
            return False
    
    # ==============================
    # STATISTICS AND MAINTENANCE
    # ==============================
    
    def get_database_stats(self):
        """Get database statistics"""
        try:
            stats = {
                "approved_users": self.db.approved_users.count_documents({}),
                "user_configs": self.db.user_config.count_documents({}),
                "user_data": self.db.user_data.count_documents({}),
                "sessions": self.db.sessions.count_documents({}),
                "session_files": self.db.session_files.count_documents({}),
                "admins": len(self.get_admins())
            }
            return stats
            
        except Exception as e:
            log.error(f"❌ Error getting database stats: {e}")
            return {}
    
    def close_connection(self):
        """Close MongoDB connection"""
        try:
            if self.client:
                self.client.close()
                log.info("✅ MongoDB connection closed")
        except Exception as e:
            log.error(f"❌ Error closing MongoDB connection: {e}")

# Global MongoDB manager instance
try:
    mongo_manager = MongoDBManager()
except Exception as e:
    log.error(f"❌ Failed to initialize MongoDB manager: {e}")
    mongo_manager = None

# Async version for compatibility with existing code
async def async_save_session_state(user_id, session_data):
    """Async wrapper for session state saving"""
    if mongo_manager:
        return mongo_manager.save_session_state(user_id, session_data)
    return False

async def async_get_session_state(user_id):
    """Async wrapper for session state retrieval"""
    if mongo_manager:
        return mongo_manager.get_session_state(user_id)
    return {}

async def async_delete_session_state(user_id):
    """Async wrapper for session state deletion"""
    if mongo_manager:
        return mongo_manager.delete_session_state(user_id)
    return False

# Async wrappers for session file management
async def async_save_session_file(user_id, session_data):
    """Async wrapper for session file saving"""
    if mongo_manager:
        return mongo_manager.save_session_file(user_id, session_data)
    return False

async def async_get_session_file(user_id):
    """Async wrapper for session file retrieval"""
    if mongo_manager:
        return mongo_manager.get_session_file(user_id)
    return None

async def async_delete_session_file(user_id):
    """Async wrapper for session file deletion"""
    if mongo_manager:
        return mongo_manager.delete_session_file(user_id)
    return False

async def async_session_file_exists(user_id):
    """Async wrapper for session file existence check"""
    if mongo_manager:
        return mongo_manager.session_file_exists(user_id)
    return False

# Async wrappers for admin management
async def async_save_admins(admin_ids):
    """Async wrapper for admin saving"""
    if mongo_manager:
        return mongo_manager.save_admins(admin_ids)
    return False

async def async_get_admins():
    """Async wrapper for admin retrieval"""
    if mongo_manager:
        return mongo_manager.get_admins()
    return set()
