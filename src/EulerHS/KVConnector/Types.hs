{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE RankNTypes          #-}
{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE DeriveAnyClass     #-}
{-# OPTIONS_GHC -Wno-star-is-type #-}
{-# OPTIONS_GHC -Wno-orphans #-}
{-# LANGUAGE DerivingStrategies #-}
{-# LANGUAGE DeriveDataTypeable #-}

module EulerHS.KVConnector.Types 
  (
    module EulerHS.KVConnector.Types,
    MeshError(..), TableMappings(..) 
  ) where

import EulerHS.Prelude
import qualified Data.Aeson as A
import           Data.Aeson.Types (Parser)
import           Data.Data (Data)
import qualified Data.HashMap.Strict as HM
import qualified Data.Map.Strict as Map
import           Data.Time (UTCTime)
import qualified EulerHS.KVDB.Language as L
import qualified Database.Beam as B
import           Database.Beam.MySQL (MySQL)
import           Database.Beam.Backend (BeamSqlBackend, HasSqlValueSyntax (sqlValueSyntax), autoSqlValueSyntax)
import qualified Database.Beam.Backend.SQL as B
import           Database.Beam.Schema (FieldModification, TableField)
import           Sequelize (Column, Set)
import qualified EulerHS.Types as T
import           Data.Aeson ((.=))
------------ TYPES AND CLASSES ------------

data PrimaryKey = PKey [(Text,Text)]
data SecondaryKey = SKey [(Text,Text)]

class KVConnector table where
  tableName :: Text
  keyMap :: HM.HashMap Text Bool -- True implies it is primary key and False implies secondary
  primaryKey :: table -> PrimaryKey
  secondaryKeys:: table -> [SecondaryKey]
  mkSQLObject :: table -> A.Value

  ----------------------------------------------

class TableMappings a where
  getTableMappings :: [(String,String)]


--------------- EXISTING DB MESH ---------------
class MeshState a where
  getShardedHashTag :: a -> Maybe Text
  getKVKey          :: a -> Maybe Text
  getKVDirtyKey     :: a -> Maybe Text
  isDBMeshEnabled   :: a -> Bool

class MeshMeta be table where
  meshModelFieldModification :: table (FieldModification (TableField table))
  valueMapper :: Map.Map Text (A.Value -> A.Value)
  parseFieldAndGetClause :: A.Value -> Text -> Parser (TermWrap be table)
  parseSetClause :: [(Text, A.Value)] -> Parser [Set be table]

data TermWrap be (table :: (* -> *) -> *) where
  TermWrap :: (B.BeamSqlBackendCanSerialize be a, A.ToJSON a, Ord a, B.HasSqlEqualityCheck be a, Show a)
              => Column table a -> a -> TermWrap be table

type MeshResult a = Either MeshError a

data MeshError
  = MKeyNotFound Text
  | MDBError T.DBError
  | MRedisError T.KVDBReply
  | MDecodingError Text
  | MUpdateFailed Text
  | MMultipleKeysFound Text
  | UnexpectedError Text
  deriving (Show, Generic, Exception, Data)

instance ToJSON MeshError where
  toJSON (MRedisError r) = A.object
    [
      "contents" A..= (show r :: Text),
      "tag" A..= ("MRedisError" :: Text)
    ]
  toJSON a = A.toJSON a

data QueryPath = KVPath | SQLPath

data MeshConfig = MeshConfig
  { meshEnabled     :: Bool
  , cerealEnabled   :: Bool
  , memcacheEnabled :: Bool
  , meshDBName      :: Text
  , ecRedisDBStream :: Text
  , kvRedis         :: Text
  , redisTtl        :: L.KVDBDuration
  , kvHardKilled    :: Bool
  }
  deriving (Generic, Eq, Show, A.ToJSON)

instance HasSqlValueSyntax MySQL String => HasSqlValueSyntax MySQL UTCTime where
  sqlValueSyntax = autoSqlValueSyntax

instance BeamSqlBackend MySQL => B.HasSqlEqualityCheck MySQL UTCTime

instance HasSqlValueSyntax MySQL String => HasSqlValueSyntax MySQL A.Value where
  sqlValueSyntax = autoSqlValueSyntax

instance BeamSqlBackend MySQL => B.HasSqlEqualityCheck MySQL A.Value

instance HasSqlValueSyntax MySQL String => HasSqlValueSyntax MySQL (Vector Int) where
  sqlValueSyntax = autoSqlValueSyntax

instance HasSqlValueSyntax MySQL String => HasSqlValueSyntax MySQL (Vector Text) where
  sqlValueSyntax = autoSqlValueSyntax

instance BeamSqlBackend MySQL => B.HasSqlEqualityCheck MySQL (Vector Int)

instance BeamSqlBackend MySQL => B.HasSqlEqualityCheck MySQL (Vector Text)

data MerchantID = MerchantID
  deriving stock (Eq, Show, Generic)
  deriving anyclass (ToJSON, FromJSON)

instance T.OptionEntity MerchantID Text

data Operation
  = CREATE
  | CREATE_RETURNING
  | UPDATE
  | UPDATE_RETURNING
  | UPDATE_ALL
  | UPDATE_ALL_RETURNING
  | FIND
  | FIND_ALL
  | FIND_ALL_WITH_OPTIONS
  | DELETE_ONE
  | DELETE_ONE_RETURNING
  | DELETE_ALL_RETURNING
  deriving (Generic, Show, ToJSON)

data Source = KV | SQL | KV_AND_SQL | IN_MEM
    deriving (Generic, Show, Eq, ToJSON)

data DBLogEntry a = DBLogEntry
  { _log_type             :: Text
  , _action               :: Text
  , _operation            :: Operation
  , _data                 :: a
  , _latency              :: Int
  , _model                :: Text
  , _cpuLatency           :: Integer
  , _source               :: Source
  , _apiTag               :: Maybe Text
  , _merchant_id          :: Maybe Text
  , _whereDiffCheckRes    :: Maybe [[Text]]
  }
  deriving stock (Generic)
  -- deriving anyclass (ToJSON)
instance (ToJSON a) => ToJSON (DBLogEntry a) where
  toJSON val = A.object [ "log_type" .= _log_type val
                        , "action" .= _action val
                        , "operation" .= _operation val
                        , "latency" .= _latency val
                        , "model" .= _model val
                        , "cpuLatency" .= _cpuLatency val
                        , "data" .= _data val
                        , "source" .= _source val
                        , "api_tag" .= _apiTag val
                        , "merchant_id" .= _merchant_id val
                        , "whereDiffCheckRes" .= _whereDiffCheckRes val
                      ]
