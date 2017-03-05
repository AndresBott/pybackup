#!/usr/bin/python3

import sys, getopt, configparser, os, ntpath, datetime, argparse
from subprocess import Popen, PIPE, call

def main(argv):
    inputfile = ''

    parser = argparse.ArgumentParser(description='Crate mysql and Files backup based on profile File definition')
    parser.add_argument('-backup', metavar='configFile',   help='run a backup job with the specified config file', type=argparse.FileType('r'))
    parser.add_argument('-restore', metavar='configFile',   help='run a restore job with the specified config file', type=argparse.FileType('r'))
    parser.add_argument('-restorefile', nargs=2 , metavar=('configFile','backupFile'),     help='run a restore job with the specified config file and the backupfile', type=argparse.FileType('r'))
    args = parser.parse_args()

    if args.backup is not None:
        isSuperCow()
        inputfile = args.backup.name
        backupparms =  {
            "profilename":"",
            "rootdir":"",
            "chown":"!",
            "chmod":"!",
            # "excludes":"",
            "mysqldb":"!",
            "mysqldbfileName":"mysqldump.sql",
            "keepold":"",
            "tmpdir":"../pybackup_tmp",
            "destination":"",
        }
        config = PyBackupConfig(inputfile,'pybackup',backupparms)
        # config.printer()
        backup = pyBackupWorker(config)
        backup.runBackup()
    elif args.restore is not None:
        isSuperCow()
        inputfile = args.restore.name
        restoreParams =  {
            "rootdir":"",
            # "excludes":"",
            "mysqldb":"!",
            "mysqldbfileName":"mysqldump.sql",
            "tmpdir":"../pybackup_tmp",
        }
        config = PyBackupConfig(inputfile,'pybackup',restoreParams)
        restore = pyBackupWorker(config)
        restore.runRestore()
    elif args.restorefile is not None:

        isSuperCow()
        configFile = args.restorefile[0].name
        restoreFile = args.restorefile[1].name

        print (configFile)
        print (restoreFile)
        print (args)



    else:
        parser.print_help()
        sys.exit(1)

def isSuperCow():
    # if not root...kick out
    if not os.geteuid()==0:
        sys.exit("\nOnly root can run this script\n")

class PyBackupConfig:

    configFile =""
    section = ""
    values= {}


    def __init__(self,file,section,defaults):
        self.configFile = os.path.abspath(file)
        self.section = section

        try:
            open(self.configFile)
            config = configparser.SafeConfigParser(defaults)
            config.read(self.configFile)


            for param in defaults:

                value = config.get(self.section, param)
                value = value.strip("'")
                value = value.strip('"')
                self.values[param] =value

        except IOError:
            print("could not read", file)
            sys.exit(1)


class pyBackupWorker():


    mysqlconfigFiles = ["/root/.my.cnf","/etc/mysql/debian.cnf"]
    tmpPath = ""

    def __init__(self,config):
        # config.printer()
        self.config = config

    def runBackup(self):
        self.createTmpDir()
        self.mysqlBackup()
        self.copyFiles()
        self.copyProfile()
        self.compressTmp()
        self.clean()
        self.cleanOld()

    def runRestore(self):
        self.createTmpDir()
        # self.mysqlRestore()

    def createTmpDir(self):

        if os.path.isabs(self.config.values["tmpdir"]):
            self.tmpPath = self.config.values["tmpdir"]
        else:
            path = self.config.values["rootdir"] + self.config.values["tmpdir"]
            self.tmpPath = os.path.abspath(path)

        # print(self.tmpPath)
        if not os.path.exists(self.tmpPath):
            os.makedirs(self.tmpPath)
            os.chmod(self.tmpPath, 0o700)

    def getMysqlConfig(self):
        ret = {}

        if self.config.values["mysqldb"] != "!":
            ret["user"] = ""
            ret["passwd"] = ""
            ret["dbName"] = self.config.values["mysqldb"]
            ret["dumpPath"] = self.tmpPath+"/"+self.config.values["mysqldbfileName"]
            mysqlconfigFiles = self.mysqlconfigFiles
            mysqlconfigFiles.append(self.config.configFile)

            for file in mysqlconfigFiles:
                if os.path.isfile(file):
                    values = {"user":"","password":""}
                    config = PyBackupConfig(file,"client",values)
                    ret["user"] = config.values["user"]
                    ret["passwd"] = config.values["password"]
                    break
        return ret


    def mysqlBackup(self):

        if self.config.values["mysqldb"] != "!":
            confdata = self.getMysqlConfig()
            args = ['mysqldump', '-u', confdata["user"], "-p"+confdata["passwd"], '--add-drop-database', '--databases', confdata["dbName"] ]
            with open(confdata["dumpPath"], 'wb', 0) as file:
                p1 = Popen(args, stdout=file)
                # p1 = Popen(args, stdout=PIPE)
                # p2 = Popen('gzip', stdin=p1.stdout, stdout=f)
            # p1.stdout.close() # force write error (/SIGPIPE) if p2 dies
            # p2.wait()
            p1.wait()

            if self.config.values["chown"] != "!":
                call(["chown",self.config.values["chown"]+":"+self.config.values["chown"],confdata["dumpPath"]])
            if self.config.values["chmod"] != "!":
                call(["chmod",self.config.values["chmod"],confdata["dumpPath"]])

    def mysqlRestore(self):
        if self.config.values["mysqldb"] != "!":
            confdata = self.getMysqlConfig()
            # mysql -u <user> -p < db_backup.dump
            args = ['mysql',  '-u', confdata["user"], "-p"+confdata["passwd"], "<", confdata["dumpPath"] ]
            print (args)

            # returncode = call(args)
            #
            # if returncode != 0:
            #     print ("something Went wrong while importing Mysql dump file")
            #     sys.exit(1)


    def copyFiles(self):
        origin =os.path.abspath(self.config.values["rootdir"]+"/.")
        destination = self.tmpPath+"/files";

        if not os.path.exists(destination):
            os.makedirs(destination)

        args = ['cp', '-a', origin, destination ]
        returncode = call(args)

        if returncode != 0:
            print ("something Went wrong while copying files with cp from: "+origin + " to: "+destination+", exiting")
            sys.exit(1)


    def copyProfile(self):
        origin =self.config.configFile
        filename =  ntpath.basename(origin)
        destination =  self.tmpPath+"/"+filename;

        args = ['cp', '-a', origin, destination ]
        returncode = call(args)

        if returncode != 0:
            print ("something Went wron while copying profile file with cp from: "+origin + " to: "+destination+", exiting")
            sys.exit(1)

    def compressTmp(self):
        now = datetime.datetime.now().strftime('%Y-%m-%d_%H.%M.%S')
        origin = self.tmpPath
        destination = self.config.values["destination"]
        # 2017_01_01_-_04_22_01.aireyvuelo_com_backup.tgz
        filename =  now+"."+self.config.values["profilename"]+".backup"
        destinationFile = destination+filename+".tgz"

        if not os.path.exists(destination):
            os.makedirs(destination)
            if self.config.values["chown"] != "!":
                call(["chown",self.config.values["chown"]+":"+self.config.values["chown"],destination])
            if self.config.values["chmod"] != "!":
                call(["chmod",self.config.values["chmod"],destination])

        # $com = 'tar '.$exclude.'-pcf '.$tarfile.' -C '.$data["rootdir"].' '.$include;
        args = ["tar", "-cpzf",destinationFile, "-C", origin, "./"]
        returncode = call(args)
        if returncode != 0:
            print ("something Went wron while Compressing TAR from: "+origin + " to: "+destinationFile+", exiting")
            sys.exit(1)

        if self.config.values["chown"] != "!":
            call(["chown",self.config.values["chown"]+":"+self.config.values["chown"],destinationFile])
        if self.config.values["chmod"] != "!":
            call(["chmod",self.config.values["chmod"],destinationFile])

    def uncompressTmp(self):

        if self.tmpPath is not "":
            args = ["tar", "-zxf",destinationFile, "-C", origin, "./"]
            returncode = call(args)
            if returncode != 0:
                print ("something Went wron while Compressing TAR from: "+origin + " to: "+destinationFile+", exiting")
                sys.exit(1)


    def clean(self):

        args = ["rm", "-R",self.tmpPath]
        returncode = call(args)
        if returncode != 0:
            print ("something Went wrong while deleting temp path: "+self.tmpPath)
            sys.exit(1)


    def cleanOld(self):
        destination = self.config.values["destination"]
        files = []
        nkeep = int(self.config.values["keepold"])

        for file in os.listdir(destination):
            if file.endswith(".tgz"):
                files.append(file)

        files.sort()
        # for file in files:
        #     print (file)
        #
        fleng = len(files)
        remove  = fleng - nkeep
        if remove <= 0:
            remove = 0

        files = files[:remove]
        # print ("===="+str(fleng))
        # for file in files:
        #     print (file)

        for delete in files:
            os.remove(destination+delete)



if __name__ == "__main__":
    main(sys.argv[1:])
