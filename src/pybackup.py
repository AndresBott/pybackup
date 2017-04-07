#!/usr/bin/python3
# version: 0.2
# author: Andres Bott <contact@andresbott.com>
# license: LGPL

import sys, getopt, configparser, os, ntpath, datetime, argparse
from subprocess import Popen, PIPE, call

def main(argv):
    inputfile = ''

    parser = argparse.ArgumentParser(description='Crate mysql and Files backup based on profile File definition')
    parser.add_argument('-backup', metavar='configFile',   help='run a backup job with the specified config file', type=argparse.FileType('r'))
    parser.add_argument('-restore', metavar='backupFile',   help='run a restore job with the specified backup file', type=argparse.FileType('r'))
    parser.add_argument('-conffile', metavar='configFile',   help='specify a different restore config file', type=argparse.FileType('r'))
    # parser.add_argument('-restorefile', nargs=2 , metavar=('configFile','backupFile'),     help='run a restore job with the specified config file and the backupfile', type=argparse.FileType('r'))
    args = parser.parse_args()

    if args.backup is not None:
        isSuperCow()
        inputfile = args.backup.name
        backupparms =  {
            "profilename":"",
            "nodate":False,
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

        if args.conffile is not None:
            inputfile = args.conffile.name
            restoreParams =  {
                "rootdir":"",
                # "excludes":"",
                "mysqldb":"!",
                "mysqldbfileName":"mysqldump.sql",
                "tmpdir":"../pybackup_tmp",
                "restoreFile":os.path.abspath(args.restore.name)
            }


            config = PyBackupConfig(inputfile,'pybackup',restoreParams)
            restore = pyBackupWorker(config)
            restore.runRestore()

        else:
            print("Not implemented yet to get config from backup included file")



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
    configinifile=""
    tmpPath = ""
    conf = {}

    def __init__(self,config):
        # config.printer()
        self.config = config.values
        self.configinifile = config.configFile

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
        self.uncompressTmp()
        self.mysqlRestore()
        self.restoreFiles()
        self.clean()

    def createTmpDir(self):

        if os.path.isabs(self.config["tmpdir"]):
            self.tmpPath = self.config["tmpdir"]
        else:
            path = self.config["rootdir"] + self.config["tmpdir"]
            self.tmpPath = os.path.abspath(path)

        # print(self.tmpPath)
        if not os.path.exists(self.tmpPath):
            os.makedirs(self.tmpPath)
            os.chmod(self.tmpPath, 0o700)

    def getMysqlConfig(self):
        ret = {}

        if self.config["mysqldb"] != "!":
            ret["user"] = ""
            ret["passwd"] = ""
            ret["dbName"] = self.config["mysqldb"]
            ret["dumpPath"] = self.tmpPath+"/"+self.config["mysqldbfileName"]
            mysqlconfigFiles = self.mysqlconfigFiles
            mysqlconfigFiles.append(self.configinifile)

            for file in mysqlconfigFiles:
                if os.path.isfile(file):
                    values = {"user":"","password":""}
                    config = PyBackupConfig(file,"client",values)
                    ret["user"] = config.values["user"]
                    ret["passwd"] = config.values["password"]
                    break
        return ret


    def mysqlBackup(self):

        if self.config["mysqldb"] != "!":
            confdata = self.getMysqlConfig()
            args = ['mysqldump', '-u', confdata["user"], "-p"+confdata["passwd"], '--add-drop-database', '--databases', confdata["dbName"] ]
            with open(confdata["dumpPath"], 'wb', 0) as file:
                p1 = Popen(args, stdout=file)
                # p1 = Popen(args, stdout=PIPE)
                # p2 = Popen('gzip', stdin=p1.stdout, stdout=f)
            # p1.stdout.close() # force write error (/SIGPIPE) if p2 dies
            # p2.wait()
            p1.wait()

            if self.config["chown"] != "!":
                call(["chown",self.config["chown"]+":"+self.config["chown"],confdata["dumpPath"]])
            if self.config["chmod"] != "!":
                call(["chmod",self.config["chmod"],confdata["dumpPath"]])

    def mysqlRestore(self):
        if self.config["mysqldb"] != "!":
            confdata = self.getMysqlConfig()


            stdin = open(confdata["dumpPath"])
            args = ['mysql', '-u', confdata["user"], "-p"+confdata["passwd"] ]
            p = Popen(args, stdin=stdin)
            p.wait()

            if p.returncode != 0:
                print ("something Went wrong while importing Mysql dump file")
                print(args)
                sys.exit(1)


    def copyFiles(self):
        origin =os.path.abspath(self.config["rootdir"]+"/.")
        destination = self.tmpPath+"/files";

        if not os.path.exists(destination):
            os.makedirs(destination)

        args = ['cp', '-a', origin, destination ]
        returncode = call(args)

        if returncode != 0:
            print ("something Went wrong while copying files with cp from: "+origin + " to: "+destination+", exiting")
            sys.exit(1)

    def restoreFiles(self):
        destination =os.path.abspath(self.config["rootdir"]+"/.")
        origin = self.tmpPath+"/files";

        args = ['rm',"-rf",destination]
        # returncode = call('rm -rf '+destination+"/*", shell=True)
        returncode = call(args)
        if returncode != 0:
            print ("something Went wrong while deleting content in path: "+destination)
            sys.exit(1)

        # args = ['cp','-rf',origin+"/*",destination]
        returncode = call('cp -arf '+origin+'/* '+destination,shell=True)
        if returncode != 0:
            print ("something Went wrong while copying files with cp from: "+origin + " to: "+destination+", exiting")
            sys.exit(1)


    def copyProfile(self):
        origin =self.configinifile
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
        destination = self.config["destination"]
        # 2017_01_01_-_04_22_01.aireyvuelo_com_backup.tgz

        if self.config["nodate"]:
            filename = self.config["profilename"]+".backup"
        else:
            filename =  now+"."+self.config["profilename"]+".backup"
        destinationFile = destination+filename+".tgz"

        if not os.path.exists(destination):
            os.makedirs(destination)
            if self.config["chown"] != "!":
                call(["chown",self.config["chown"]+":"+self.config["chown"],destination])
            if self.config["chmod"] != "!":
                call(["chmod",self.config["chmod"],destination])

        # $com = 'tar '.$exclude.'-pcf '.$tarfile.' -C '.$data["rootdir"].' '.$include;
        args = ["tar", "-cpzf",destinationFile, "-C", origin, "./"]
        returncode = call(args)
        if returncode != 0:
            print ("something Went wron while Compressing TAR from: "+origin + " to: "+destinationFile+", exiting")
            sys.exit(1)

        if self.config["chown"] != "!":
            call(["chown",self.config["chown"]+":"+self.config["chown"],destinationFile])
        if self.config["chmod"] != "!":
            call(["chmod",self.config["chmod"],destinationFile])

    def uncompressTmp(self):

        if self.tmpPath is not "":
            if os.path.exists(self.config["restoreFile"]):
                args = ["tar", "-zxf",self.config["restoreFile"], "-C", self.tmpPath]
                returncode = call(args)

                if returncode != 0:
                    print ("something Went wring while uncompressing "+self.config["restoreFile"] )
                    sys.exit(1)


    def clean(self):

        args = ["rm", "-R",self.tmpPath]
        returncode = call(args)
        if returncode != 0:
            print ("something Went wrong while deleting temp path: "+self.tmpPath)
            sys.exit(1)


    def cleanOld(self):
        destination = self.config["destination"]
        files = []
        nkeep = int(self.config["keepold"])

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
