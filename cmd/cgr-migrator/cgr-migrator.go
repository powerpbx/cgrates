/*
Real-time Online/Offline Charging System (OCS) for Telecom & ISP environments
Copyright (C) ITsysCOM GmbH

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program.  If not, see <http://www.gnu.org/licenses/>
*/

package main

import (
	"flag"
	"fmt"
	"log"
	"strings"

	"github.com/cgrates/cgrates/config"
	"github.com/cgrates/cgrates/engine"
	"github.com/cgrates/cgrates/migrator"
	"github.com/cgrates/cgrates/utils"
)

var (
	sameDataDB, sameStorDB bool
	dmIN, dmOUT            migrator.MigratorDataDB
	storDBIn, storDBOut    migrator.MigratorStorDB
	err                    error
	dfltCfg, _             = config.NewDefaultCGRConfig()
	cfgDir                 = flag.String("config_dir", "",
		"Configuration directory path.")

	migrate = flag.String("migrate", "", "fire up automatic migration "+
		"\n <*set_versions|*cost_details|*accounts|*actions|*action_triggers|*action_plans|*shared_groups|*stordb|*datadb>")
	version = flag.Bool("version", false, "prints the application version")

	inDataDBType = flag.String("datadb_type", dfltCfg.DataDbType,
		"the type of the DataDB Database <*redis|*mongo>")
	inDataDBHost = flag.String("datadb_host", dfltCfg.DataDbHost,
		"the DataDB host")
	inDataDBPort = flag.String("datadb_port", dfltCfg.DataDbPort,
		"the DataDB port")
	inDataDBName = flag.String("datadb_name", dfltCfg.DataDbName,
		"the name/number of the DataDB")
	inDataDBUser = flag.String("datadb_user", dfltCfg.DataDbUser,
		"the DataDB user")
	inDataDBPass = flag.String("datadb_passwd", dfltCfg.DataDbPass,
		"the DataDB password")
	inDBDataEncoding = flag.String("dbdata_encoding", dfltCfg.DBDataEncoding,
		"the encoding used to store object Data in strings")
	inDataDBRedisSentinel = flag.String("redis_sentinel", dfltCfg.DataDbSentinelName,
		"the name of redis sentinel")

	outDataDBType = flag.String("out_datadb_type", utils.MetaDataDB,
		"output DataDB type <*redis|*mongo>")
	outDataDBHost = flag.String("out_datadb_host", utils.MetaDataDB,
		"output DataDB host to connect to")
	outDataDBPort = flag.String("out_datadb_port", utils.MetaDataDB,
		"output DataDB port")
	outDataDBName = flag.String("out_datadb_name", utils.MetaDataDB,
		"output DataDB name/number")
	outDataDBUser = flag.String("out_datadb_user", utils.MetaDataDB,
		"output DataDB user")
	outDataDBPass = flag.String("out_datadb_passwd", utils.MetaDataDB,
		"output DataDB password")
	outDBDataEncoding = flag.String("out_dbdata_encoding", utils.MetaDataDB,
		"the encoding used to store object Data in strings in move mode")
	outDataDBRedisSentinel = flag.String("out_redis_sentinel", utils.MetaDataDB,
		"the name of redis sentinel")

	inStorDBType = flag.String("stordb_type", dfltCfg.StorDBType,
		"the type of the StorDB Database <*mysql|*postgres|*mongo>")
	inStorDBHost = flag.String("stordb_host", dfltCfg.StorDBHost,
		"the StorDB host")
	inStorDBPort = flag.String("stordb_port", dfltCfg.StorDBPort,
		"the StorDB port")
	inStorDBName = flag.String("stordb_name", dfltCfg.StorDBName,
		"the name/number of the StorDB")
	inStorDBUser = flag.String("stordb_user", dfltCfg.StorDBUser,
		"the StorDB user")
	inStorDBPass = flag.String("stordb_passwd", dfltCfg.StorDBPass,
		"the StorDB password")

	outStorDBType = flag.String("out_stordb_type", utils.MetaStorDB,
		"output StorDB type for move mode <*mysql|*postgres|*mongo>")
	outStorDBHost = flag.String("out_stordb_host", utils.MetaStorDB,
		"output StorDB host")
	outStorDBPort = flag.String("out_stordb_port", utils.MetaStorDB,
		"output StorDB port")
	outStorDBName = flag.String("out_stordb_name", utils.MetaStorDB,
		"output StorDB name/number")
	outStorDBUser = flag.String("out_stordb_user", utils.MetaStorDB,
		"output StorDB user")
	outStorDBPass = flag.String("out_stordb_passwd", utils.MetaStorDB,
		"output StorDB password")

	dryRun = flag.Bool("dry_run", false,
		"parse loaded data for consistency and errors, without storing it")
	verbose = flag.Bool("verbose", false, "enable detailed verbose logging output")
)

func main() {
	flag.Parse()
	if *version {
		fmt.Println(utils.GetCGRVersion())
		return
	}

	mgrCfg := dfltCfg
	if *cfgDir != "" {
		if mgrCfg, err = config.NewCGRConfigFromFolder(*cfgDir); err != nil {
			log.Fatalf("error loading config file %s", err.Error())
		}
	}

	// inDataDB
	if *inDataDBType != dfltCfg.DataDbType {
		mgrCfg.DataDbType = strings.TrimPrefix(*inDataDBType, "*")
	}
	if *inDataDBHost != dfltCfg.DataDbHost {
		mgrCfg.DataDbHost = *inDataDBHost
	}
	if *inDataDBPort != dfltCfg.DataDbPort {
		mgrCfg.DataDbPort = *inDataDBPort
	}
	if *inDataDBName != dfltCfg.DataDbName {
		mgrCfg.DataDbName = *inDataDBName
	}
	if *inDataDBUser != dfltCfg.DataDbUser {
		mgrCfg.DataDbUser = *inDataDBUser
	}
	if *inDataDBPass != dfltCfg.DataDbPass {
		mgrCfg.DataDbPass = *inDataDBPass
	}
	if *inDBDataEncoding != dfltCfg.DBDataEncoding {
		mgrCfg.DBDataEncoding = *inDBDataEncoding
	}
	if *inDataDBRedisSentinel != dfltCfg.DataDbSentinelName {
		mgrCfg.DataDbSentinelName = *inDataDBRedisSentinel
	}

	// outDataDB
	if *outDataDBType == utils.MetaDataDB {
		if dfltCfg.MigratorCgrConfig.OutDataDBType == mgrCfg.MigratorCgrConfig.OutDataDBType {
			mgrCfg.MigratorCgrConfig.OutDataDBType = mgrCfg.DataDbType
		}
	} else {
		mgrCfg.MigratorCgrConfig.OutDataDBType = strings.TrimPrefix(*outDataDBType, "*")
	}

	if *outDataDBHost == utils.MetaDataDB {
		if dfltCfg.MigratorCgrConfig.OutDataDBHost == mgrCfg.MigratorCgrConfig.OutDataDBHost {
			mgrCfg.MigratorCgrConfig.OutDataDBHost = mgrCfg.DataDbHost
		}
	} else {
		mgrCfg.MigratorCgrConfig.OutDataDBHost = *outDataDBHost
	}
	if *outDataDBPort == utils.MetaDataDB {
		if dfltCfg.MigratorCgrConfig.OutDataDBPort == mgrCfg.MigratorCgrConfig.OutDataDBPort {
			mgrCfg.MigratorCgrConfig.OutDataDBPort = mgrCfg.DataDbPort
		}
	} else {
		mgrCfg.MigratorCgrConfig.OutDataDBPort = *outDataDBPort
	}
	if *outDataDBName == utils.MetaDataDB {
		if dfltCfg.MigratorCgrConfig.OutDataDBName == mgrCfg.MigratorCgrConfig.OutDataDBName {
			mgrCfg.MigratorCgrConfig.OutDataDBName = mgrCfg.DataDbName
		}
	} else {
		mgrCfg.MigratorCgrConfig.OutDataDBName = *outDataDBName
	}
	if *outDataDBUser == utils.MetaDataDB {
		if dfltCfg.MigratorCgrConfig.OutDataDBUser == mgrCfg.MigratorCgrConfig.OutDataDBUser {
			mgrCfg.MigratorCgrConfig.OutDataDBUser = mgrCfg.DataDbUser
		}
	} else {
		mgrCfg.MigratorCgrConfig.OutDataDBUser = *outDataDBUser
	}
	if *outDataDBPass == utils.MetaDataDB {
		if dfltCfg.MigratorCgrConfig.OutDataDBPassword == mgrCfg.MigratorCgrConfig.OutDataDBPassword {
			mgrCfg.MigratorCgrConfig.OutDataDBPassword = mgrCfg.DataDbPass
		}
	} else {
		mgrCfg.MigratorCgrConfig.OutDataDBPassword = *outDataDBPass
	}
	if *outDBDataEncoding == utils.MetaDataDB {
		if dfltCfg.MigratorCgrConfig.OutDataDBEncoding == mgrCfg.MigratorCgrConfig.OutDataDBEncoding {
			mgrCfg.MigratorCgrConfig.OutDataDBEncoding = mgrCfg.DBDataEncoding
		}
	} else {
		mgrCfg.MigratorCgrConfig.OutDataDBEncoding = *outDBDataEncoding
	}
	if *outDataDBRedisSentinel == utils.MetaDataDB {
		if dfltCfg.MigratorCgrConfig.OutDataDBRedisSentinel == mgrCfg.MigratorCgrConfig.OutDataDBRedisSentinel {
			mgrCfg.MigratorCgrConfig.OutDataDBRedisSentinel = mgrCfg.DBDataEncoding
		}
	} else {
		mgrCfg.MigratorCgrConfig.OutDataDBRedisSentinel = *outDataDBRedisSentinel
	}

	sameDataDB = mgrCfg.MigratorCgrConfig.OutDataDBType == mgrCfg.DataDbType &&
		mgrCfg.MigratorCgrConfig.OutDataDBHost == mgrCfg.DataDbHost &&
		mgrCfg.MigratorCgrConfig.OutDataDBPort == mgrCfg.DataDbPort &&
		mgrCfg.MigratorCgrConfig.OutDataDBName == mgrCfg.DataDbName &&
		mgrCfg.MigratorCgrConfig.OutDataDBEncoding == mgrCfg.DBDataEncoding

	if dmIN, err = migrator.NewMigratorDataDB(mgrCfg.DataDbType,
		mgrCfg.DataDbHost, mgrCfg.DataDbPort,
		mgrCfg.DataDbName, mgrCfg.DataDbUser,
		mgrCfg.DataDbPass, mgrCfg.DBDataEncoding,
		mgrCfg.CacheCfg(), mgrCfg.DataDbSentinelName); err != nil {
		log.Fatal(err)
	}

	if sameDataDB {
		dmOUT = dmIN
	} else if dmOUT, err = migrator.NewMigratorDataDB(mgrCfg.MigratorCgrConfig.OutDataDBType,
		mgrCfg.MigratorCgrConfig.OutDataDBHost, mgrCfg.MigratorCgrConfig.OutDataDBPort,
		mgrCfg.MigratorCgrConfig.OutDataDBName, mgrCfg.MigratorCgrConfig.OutDataDBUser,
		mgrCfg.MigratorCgrConfig.OutDataDBPassword, mgrCfg.MigratorCgrConfig.OutDataDBEncoding,
		mgrCfg.CacheCfg(), mgrCfg.MigratorCgrConfig.OutDataDBRedisSentinel); err != nil {
		log.Fatal(err)
	}

	if *outStorDBType == utils.MetaDynamic {
		storDB = instorDB
	} else {
		storDB, err = engine.ConfigureStorStorage(*outStorDBType, *outStorDBHost, *outStorDBPort, *outStorDBName, *outStorDBUser, *outStorDBPass, *dbDataEncoding,
			config.CgrConfig().StorDBMaxOpenConns, config.CgrConfig().StorDBMaxIdleConns, config.CgrConfig().StorDBConnMaxLifetime, config.CgrConfig().StorDBCDRSIndexes)
		if err != nil {
			log.Fatal(err)
		}
	}
	if *inDataDBName != *outDataDBName || *inDataDBType != *outDataDBType || *inDataDBHost != *outDataDBHost {
		sameDataDB = false
	}
	if *inStorDBName != *outStorDBName || *inStorDBType != *outStorDBName || *inStorDBHost != *outStorDBHost {
		sameStorDB = false
	}
	m, err := migrator.NewMigrator(dmIN, dmOUT, *inDataDBType, *dbDataEncoding, storDB, *inStorDBType, outDataDB,
		*outDataDBType, *inDBDataEncoding, instorDB, *outStorDBType, *dryRun, sameDataDB, sameStorDB, *datadb_versions, *stordb_versions)
	if err != nil {
		log.Fatal(err)
	}
	if *datadb_versions {
		vrs, _ := dmOUT.DataDB().GetVersions(utils.TBLVersions)
		if len(vrs) != 0 {
			log.Printf("DataDB versions : %+v\n", vrs)
		} else {
			log.Printf("DataDB versions not_found")
		}
	}
	if *stordb_versions {
		vrs, _ := storDB.GetVersions(utils.TBLVersions)
		if len(vrs) != 0 {
			log.Printf("StorDB versions : %+v\n", vrs)
		} else {
			log.Printf("StorDB versions not_found")
		}
	}
	if migrate != nil && *migrate != "" { // Run migrator
		migrstats := make(map[string]int)
		mig := strings.Split(*migrate, ",")
		err, migrstats = m.Migrate(mig)
		if err != nil {
			log.Fatal(err)
		}
		if *verbose != false {
			log.Printf("Data migrated: %+v", migrstats)
		}
		return
	}

}
