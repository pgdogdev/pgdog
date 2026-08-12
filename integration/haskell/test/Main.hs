{-# LANGUAGE DataKinds #-}
{-# LANGUAGE DerivingStrategies #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE QuasiQuotes #-}
{-# LANGUAGE StandaloneDeriving #-}
{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE UndecidableInstances #-}

module Main (main) where

import Control.Monad.IO.Class (liftIO)
import Control.Monad.Logger (runNoLoggingT)
import Data.Text (Text)
import Database.Persist
import Database.Persist.Postgresql
import Database.Persist.TH
import Test.Hspec

share
    [mkPersist sqlSettings, mkMigrate "migrateAll"]
    [persistLowerCase|
PersistentThing sql=haskell_persistent
    value Text
    deriving Eq Show
|]

connectionString :: ConnectionString
connectionString = "host=127.0.0.1 port=6432 user=pgdog password=pgdog dbname=pgdog sslmode=disable"

main :: IO ()
main = runNoLoggingT $
    withPostgresqlPool connectionString 2 $ \pool ->
        liftIO $ do
            runSqlPool (runMigration migrateAll) pool
            hspec $
                before_ (runSqlPool clearTestRows pool) $
                    describe "Persistent through PgDog" $ do
                        it "connects and runs a typed query" $ do
                            result <- runSqlPool selectOne pool
                            result `shouldBe` [Single 1]

                        it "creates, reads, updates, and deletes a row" $
                            runSqlPool basicCrud pool

clearTestRows :: SqlPersistT IO ()
clearTestRows = deleteWhere ([] :: [Filter PersistentThing])

selectOne :: SqlPersistT IO [Single Int]
selectOne = rawSql "SELECT 1" []

basicCrud :: SqlPersistT IO ()
basicCrud = do
    key <- insert (PersistentThing "created")

    created <- get key
    liftIO $ created `shouldBe` Just (PersistentThing "created")

    update key [PersistentThingValue =. "updated"]
    updated <- get key
    liftIO $ updated `shouldBe` Just (PersistentThing "updated")

    delete key
    deleted <- get key
    liftIO $ deleted `shouldBe` Nothing
