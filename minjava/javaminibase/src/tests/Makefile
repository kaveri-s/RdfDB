# modify the paths appropriately
#JDKPATH = /usr/lib/jvm/java-1.8.0-openjdk-1.8.0.322.b06-1.el7_9.x86_64/
LIBPATH = .:..
CLASSPATH = $(LIBPATH)
BINPATH = $(JDKPATH)/bin
JAVAC = $(JDKPATH)/bin/javac -classpath $(CLASSPATH)
JAVA  = $(JDKPATH)/bin/java  -classpath $(CLASSPATH)

# DB Phase 2 testing
Batchinsert:Batchinsert.java
	$(JAVAC) Batchinsert.java

Report:Report.java
	$(JAVAC) Report.java

QueryProgram:QueryProgram.java
	$(JAVAC) QueryProgram.java

batchinsert: Batchinsert
	$(JAVA) tests.Batchinsert $(DATAFILE) $(INDEXOPTION) $(DBNAME)

queryprogram: QueryProgram
	$(JAVA) tests.QueryProgram $(DBNAME) $(QUERYFILE) $(NUMBUFRDF)

report: Report
	$(JAVA) tests.Report $(DBNAME) $(INDEXOPTION)


