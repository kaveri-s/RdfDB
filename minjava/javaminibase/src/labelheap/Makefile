#JDKPATH = /usr/lib/jvm/java-1.8.0-openjdk-1.8.0.322.b06-1.el7_9.x86_64/
LIBPATH = .:..
CLASSPATH = $(JDKPATH)/lib/classes.zip:.:..:$(LIBPATH)
BINPATH = $(JDKPATH)/bin
JAVAC = $(JDKPATH)/bin/javac -classpath $(CLASSPATH)
JAVA  = $(JDKPATH)/bin/java  -classpath $(CLASSPATH)

PROGS = together

all: $(PROGS)

lhf:LabelHeapfile.java
	$(JAVAC) LabelHeapfile.java

lhp:LHFPage.java
	$(JAVAC) LFPage.java

label:Label.java
	$(JAVAC) Label.java

lscan:LScan.java
	$(JAVAC) LScan.java

together:*.java
	$(JAVAC) *.java

clean:
	\rm -f *.class *~ \#* core
