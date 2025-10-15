#!/usr/bin/env perl

# ============================================================================
#
# Name    :  rnx2snx_pcs.pl
#
# Purpose :  Start RNX2SNX BPE process for a particular session
#
# Author  :  R. Dach
# Created :  07-Jun-2022
#
# Changes :  07-Jun-2022 RD: Created for version 5.4
#
# ============================================================================
use strict;

use lib $ENV{BPE};
use startBPE;
use bpe_util;

# Check arguments
# ---------------
if (@ARGV != 2 or lc($ARGV[0]) eq "-h") {
  die "\n  Start RNX2SNX BPE process for a particular session\n".
      "\n  Usage: rnx2snx_pcs.pl [-h] yyyy ssss\n".
      "\n  yyyy : 4-digit (or 2-digit) year".
      "\n  ssss : 4-character session".
      "\n  -h   : Display this help text\n\n" }

# Create startBPE object
# ----------------------
my $bpe = new startBPE();

# Redefine mandatory variables
# ----------------------------
$$bpe{PCF_FILE}     = "FINAL";
$$bpe{CPU_FILE}     = "USER";
$$bpe{BPE_CAMPAIGN} = "FINAL";
$$bpe{YEAR}         = $ARGV[0];
$$bpe{SESSION}      = $ARGV[1];
$$bpe{SYSOUT}       = "RNX2SNX";
$$bpe{STATUS}       = "RNX2SNX.RUN";
$$bpe{TASKID}       = "RS";

# Reset CPU file
# --------------
$bpe->resetCPU();

# Start BPE process
# -----------------
print "\nRNX2SNX BPE process started on ".timstr(localtime(time))."\n";

# The BPE runs
# ------------
$bpe->run();

# Check for error
# ---------------
if ($$bpe{ERROR_STATUS} ) {
  die ("Error in EXAMPLE BPE: $$bpe{PCF_FILE}.PCF (Session: $ARGV[1])\n");
}

# BPE process finished
# --------------------
print "RNX2SNX BPE process finished on ".timstr(localtime(time))."\n\n";

__END__

