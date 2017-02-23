#!/usr/bin/python3

import sys, getopt, configparser, os, ntpath, datetime, argparse
from subprocess import Popen, PIPE, call

def main(argv):
    inputfile = ''

    parser = argparse.ArgumentParser(description='Crate mysql and Files backup based on profile File definition')
    parser.add_argument('-run', metavar='configFile',   help='run a backup job with the specified config file', type=argparse.FileType('r'))
    parser.add_argument('-restore', metavar='restorePath',   help='Restore a tgz compressed with pybackuo to the desired path', type=argparse.FileType('r'))
    args = parser.parse_args()

    if args.run is not None:
        isSuperCow()
        inputfile = args.run.name
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
        config = BackupConfig(inputfile,'pybackup',backupparms)
        # config.printer()
        backup = BackupWorker(config)
        backup.run()

    else:
        parser.print_help()
        sys.exit(1)

def isSuperCow():
    # if not root...kick out
    if not os.geteuid()==0:
        sys.exit("\nOnly root can run this script\n")


class BackupConfig:
    """
    Class To handle the needed configuration
    will read a inifile in the consturctor and fill in the dictionoary with the needed values
    if a value is defined it will assign it in the exception
    """

    configFile =""
    section = ""
    values= {}


    def __init__(self,file,section,values):
        """
        parse a inifile and add the defined values to the config instance
        :param file: file to be loaded
        :return: none
        """
        self.configFile = os.path.abspath(file)
        self.section = section
        self.values =  values

        try:
            open(file)
            config = configparser.ConfigParser()
            config.read(file)

            for param in self.values:
                try:
                    value = config.get(self.section, param)
                    value = value.strip("'")
                    value = value.strip('"')
                    self.values[param] =value
                except configparser.NoOptionError:
                    if self.values[param] == "" :
                        e = str(sys.exc_info()[1])
                        print("Error: "+e);
                        sys.exit(1)
        except IOError:
            print("could not read", file)
            sys.exit(1)

        # except: # catch *all* exceptions
        #     e = str(sys.exc_info()[0])
        #     print("Unexpected error: "+e);
        #     sys.exit(1)

    def printer(self):
        """
        Print out the values
        :return: none
        """
        print ("====================")
        for param, value in self.values.items() :
            print (param+" : "+value)
        print ("====================")


class BackupWorker:


    mysqlconfigFiles = ["/root/.my.cnf","/etc/mysql/debian.cnf"]
    tmpPath = ""

    def __init__(self,config):
        # config.printer()
        self.config = config

    def run(self):
        self.createTmpDir()
        self.mysqlBackup()
        self.copyFiles()
        self.copyProfile()
        self.compressTmp()
        self.clean()
        self.cleanOld()

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

    def mysqlBackup(self):

        if self.config.values["mysqldb"] != "!":
            user = ""
            passwd = ""
            dbName = self.config.values["mysqldb"]
            dumpPath = self.tmpPath+"/"+self.config.values["mysqldbfileName"]
            mysqlconfigFiles = self.mysqlconfigFiles
            mysqlconfigFiles.append(self.config.configFile)

            for file in mysqlconfigFiles:
                if os.path.isfile(file):
                    values = {"user":"","password":""}
                    config = BackupConfig(file,"client",values)
                    user = config.values["user"]
                    passwd = config.values["password"]
                    break

            args = ['mysqldump', '-u', user, "-p"+passwd, '--add-drop-database', '--databases', dbName ]

            with open(dumpPath, 'wb', 0) as file:
                p1 = Popen(args, stdout=file)
                # p1 = Popen(args, stdout=PIPE)
                # p2 = Popen('gzip', stdin=p1.stdout, stdout=f)
            # p1.stdout.close() # force write error (/SIGPIPE) if p2 dies
            # p2.wait()
            p1.wait()

            if self.config.values["chown"] != "!":
                call(["chown",self.config.values["chown"]+":"+self.config.values["chown"],dumpPath])
            if self.config.values["chmod"] != "!":
                call(["chmod",self.config.values["chmod"],dumpPath])


    def copyFiles(self):
        origin =os.path.abspath(self.config.values["rootdir"]+"/.")
        destination = self.tmpPath+"/files";

        if not os.path.exists(destination):
            os.makedirs(destination)

        args = ['cp', '-a', origin, destination ]
        returncode = call(args)

        if returncode != 0:
            print ("something Went wron while copying files with cp from: "+origin + " to: "+destination+", exiting")
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
        now = datetime.datetime.now().strftime('%Y-%m-%d=%H.%M.%S')
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
