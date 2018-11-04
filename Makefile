FPM_EXTRA_ARGS := $(FPM_EXTRA_ARGS) -d perl-File-Slurp

.PHONY: all run-test copy-files

all: # nothing to build

# Include here so that the default target is "all"
include cicd.mk

run-test:

# INSTDIR is the "Install dir" used by the build system
copy-files:
	mkdir -p $(INSTDIR)/opt/$(NAME)
	rsync -avr *.pl $(INSTDIR)/opt/$(NAME)

create-package: copy-files

