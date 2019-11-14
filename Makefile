#######################################################
# generic java compile Makefile
#######################################################
# Features:
# 1. source files modification check
# 2. jar file modification check
# 3. append jar files to -classpath
# 4. run program with arguments
# 5. auto-detect main and run program with arguments

#######################################################
# Usage
#######################################################
# make [all|build|prog|run|clean|dist-clean]
#
# all: compile and compress jar
# build: compile
# prog: run with arguments
# run: auto-detect main and run with arguments 
# clean: remove classes
# dist-clean: remove classes and temporary files

#######################################################
# Customize enviroments
#######################################################
CLASSES_DIR := classes
SRC_DIR := src
LIB_DIR := lib

JAVAC := javac
JAVA := java
RM := /bin/rm -f
JARFILE := starconv.jar

#######################################################
# append jar files to classes path in $(LIB_DIR)
#######################################################
LIB_PATH=$(subst $(eval) ,:,$(wildcard $(LIB_DIR)/*.jar))
ifneq ($(LIB_PATH),"")
LIB_PATH:=:$(LIB_PATH)
endif

JAVAC_FLAGS = -classpath $(SRC_DIR)$(LIB_PATH) -d $(CLASSES_DIR)
JAVA_FLAGS = -classpath $(CLASSES_DIR)$(LIB_PATH)

#######################################################
# Common java compile configurations
#######################################################

current_makefile := $(abspath $(lastword $(MAKEFILE_LIST)))

findMain = $(subst /,.,$(patsubst $(SRC_DIR)/%.java,%,$(shell egrep "^\s*(\s+(static|public|void)){3,}\s+main\s*\(\s*String\s*\[\]\s+\w+\)" $(SRC_DIR) -r|cut -d ":" -f1)))

RUN_ARGS := $(wordlist 2,$(words $(MAKECMDGOALS)),$(MAKECMDGOALS))

.SUFFIXES: .java .class

CLASSES = $(shell find $(SRC_DIR) -type f -name *.java|sed 's!^src!$(CLASSES_DIR)!g;s!\.java!.class!g'|tr '\n' ' ')

get_source = $(patsubst $(CLASSES_DIR)/%,$(SRC_DIR)/%,$(patsubst %.class,%.java,$(1)))

all: $(JARFILE)

build: validate-srcs $(CLASSES)

run : build
	$(JAVA) $(JAVA_FLAGS) $(findMain) $(RUN_ARGS)

prog : build
	$(JAVA) $(JAVA_FLAGS) $(RUN_ARGS)

$(CLASSES):
	$(JAVAC) $(JAVAC_FLAGS) $(call get_source, $@)

clean:
	$(RM) $(CLASSES)

validate-srcs:
	@sed -n '/^#begin_validate_srcs:/,/^#end_validate_srcs:/p' $(current_makefile) | env JARFILE=$(JARFILE) SRC_DIR=$(SRC_DIR) CLASSES_DIR=$(CLASSES_DIR) sh

jarcheck:
	@sed -n '/^#begin_jarcheck:/,/^#end_jarcheck:/p' $(current_makefile) | env JARFILE=$(JARFILE) SRC_DIR=$(SRC_DIR) CLASSES_DIR=$(CLASSES_DIR) sh 

dist-clean: clean
	$(RM) .md5sum .md5jar .updated

$(JARFILE): validate-srcs jarcheck $(CLASSES)
	-@if [ ! -f ".md5jar" -o -f .updated ]; then echo "Update jar: $@"; jar -cf $@ -C $(CLASSES_DIR) .; fi
	-@md5sum $@>.md5jar

.PHONY:
	build clean validate-srcs prog run

ifeq ("block","comment")
#begin_jarcheck:
	if [ ! -f "${JARFILE}" ]; then 
	/bin/rm -f .md5jar
	elif [ -f ".md5jar" ]; then
	if [ "$(md5sum ${JARFILE})" != "$(cat .md5jar)" ]; then
	/bin/rm -f .md5jar
	fi
	else
	md5sum "${JARFILE}">.md5jar
	fi
#end_jarcheck:

#begin_validate_srcs:
	gen_md5sum() {
	/bin/rm -f .md5sum
	find ${SRC_DIR} -name "*.java"|xargs -l md5sum >> .md5sum
	touch .updated
	}
	if [ ! -f .md5sum ]; then
	gen_md5sum
	else
	updated=0
	for f in $(find ${SRC_DIR} -name "*.java");
	do
	MD5=$(grep -F "$f" .md5sum)
	if [ -n "${MD5}" ]; then
	if [ "${MD5}" != "$(md5sum $f)" ]; then
	updated=1
	f="${CLASSES_DIR}/${f#${SRC_DIR}/}"
	f="${f%.java}.class"
	/bin/rm -f "$f"
	fi
	else
	updated=1
	fi
	done
	[ ${updated} -eq 1 ] && gen_md5sum || /bin/rm -f .updated
	fi
	exit 0
#end_validate_srcs:
endif
