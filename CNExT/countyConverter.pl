###########
##
## custom perl script "countyConverter", addresses post processing
## conversion of fips codes for counties into textual form.
## leverages standards in county-fips-master file.
## by inigo on march 2022.
##########
use strict;
my $line;  my $i=0;
my  $source  = 'D:\\KRIIS_ETLs\\Sources\\cnext\\cnext_location.dat';
my $fileFIPS = 'D:\\KRIIS_ETLs\\Queries\\CNExT_Extractions\\county_fips_master.csv';
my $outfile=$source.'N';  my $ii; my $item;

open(F, $source) or die "couldnt open the $source file \n";
open(FOU,">$outfile") or die "couldnt open this $outfile \n";

my @els; my $countycode; my $state; my $statecode;
my %fipscounty; my $fipscodecounty;  my %fipsstate; my $fipscodestate;

open(FIPS,$fileFIPS) or die "couldnt opn county_fips_master csv\n";

while ($line=<FIPS>){
  @els=split(/,/,$line);
  $fipscounty{$els[0]}=$els[1];  
  $fipsstate{$els[2]} =$els[8];
}
close(FIPS); 
while ($line=<F>){
   @els = split(/\|/,$line);
   $countycode = $els[8];        ##...|CITY|STATE|ZIP|COUNTY|...
   if ($countycode>=1) {
        $state = $els[6]; #  print "state, $state\n"; 	
        if(($state=~/ZZ/)or($state=~/XX/)or($state=~/YY/)){
            # out of country
        }else{
            $fipscodecounty=$fipsstate{$state}.$countycode; 
        }
        if ($fipscounty{$fipscodecounty}){
           $els[8]=$fipscounty{$fipscodecounty};
        }
      
   }
   for( $ii = 0; $ii < $#els; $ii++){
        print FOU $els[$ii].'|';
   }
   print FOU $els[$#els];
} 
close(FOU);
close(F);


  