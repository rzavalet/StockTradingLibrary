#!/usr/bin/perl

use strict;
use warnings;

my @tests = ('test1', 'test2');
my $test_number = 0;
my $test_passed = 0;
my $test_failed = 0;
my $cmd;
my $rc;

sub setupTest
{
  my $cmd;
  my $rc;

  $cmd = "rm -rf /tmp/cnt*";
  $rc = system($cmd);
  if ($rc != 0) {
    print "ERROR: Failed to cleanup\n";
    return 1;
  }

  $cmd = "rm -rf /tmp/upd*";
  $rc = system($cmd);
  if ($rc != 0) {
    print "ERROR: Failed to cleanup\n";
    return 1;
  }

  $cmd = "rm -rf /tmp/chronos/databases";
  $rc = system($cmd);
  if ($rc != 0) {
    print "ERROR: Failed to cleanup\n";
    return 1;
  }

  $cmd = "rm -rf /tmp/chronos/datafiles";
  $rc = system($cmd);
  if ($rc != 0) {
    print "ERROR: Failed to cleanup\n";
    return 1;
  }

  $cmd = "mkdir /tmp/chronos/databases";
  $rc = system($cmd);
  if ($rc != 0) {
    print "ERROR: Failed to cleanup\n";
    return 1;
  }

  $cmd = "mkdir /tmp/chronos/datafiles";
  $rc = system($cmd);
  if ($rc != 0) {
    print "ERROR: Failed to cleanup\n";
    return 1;
  }

  $cmd = "cp ../datafiles/* /tmp/chronos/datafiles";
  $rc = system($cmd);
  if ($rc != 0) {
    print "ERROR: Failed to cleanup\n";
    return 1;
  }

  return 0;
}

foreach my $test (@tests) {
  $test_number ++;

  print "\n";
  print "++ Running test ($test_number): $test\n";

  $rc = setupTest();
  if ($rc != 0) {
    print "ERROR: Failed to setup test\n";
    $test_failed ++;
    last;
  }

  $cmd = "./$test";
  $rc = system($cmd);

  if ($rc != 0) {
    $test_failed ++;
  }
  else {
    $test_passed ++;
  }

}

print "\n";
print "============================================================\n";
print "PASSED=$test_passed  FAILED=$test_failed  TOTAL=$test_number\n";
print "============================================================\n";
