#!/usr/bin/perl
##
##  This Perl script extracts Foundation Medcine data from XML report files
##  and prepares it for load to the ORacle CRIIS OCI LDS.
##  Data contains the report of the genomic profiles
##  Cannot use LibXML cause some tags have hyphens-crap
##  Parses in order (top-down), relies on well-formed, schema compliant XMLs.
##   two steps:
##     1) file scanning and extraction in hashes.
##     2) materializing data in LDS conforming mode.
##
##  Xcuse the fast-n-dirty.  As Perl motto goes "I dont need to outrun the ##  bear; I need to outrun you.
##
##  Gist -- cannot use standard XML libs, as FMI XML violates basic XML rules
##  regarding labels. Instead of sanitizing fields, I write my own parser,
##  which is very procedural. We read XML reports from top to bottom, and seek
##  data elements as we progress, using a semaphore system (conditionals + flags)
##  to understand what to look for next.
##
##  Once data are captured in hashes, it outputs conforming to the data warehouse
##  landing data store formats, a crude stage before our OMOP implementation
## packages
use 5.010;
use Time::Piece;
use strict;
use warnings;
## Declarations - not fully cleaned
my $line; my $filename ;
my $lookingReportID=1; my $lookingMRN = 0; my $lookingTestType = 0;
my $lookingFirstName = 0; my $lookingLastName = 0;
my $lookingGender = 0; my $lookingDOB = 0;
my $lookingOrderingMD = 0; my $lookingRecDate = 0 ; my $lookingOrderingMDid =0;
my $lookingSpecSite = 0;  my $lookingSpecFormat = 0;
my $lookingSubDX = 0; my $lookingSummCounts = 0;
my $lookingSCopyNumber = 0; my $lookingSGenes = 0;
my $lookingVarProps = 0; my $lookingGeneS = 0;
my $lookingVarProperties = 0 ; my $lookingNPI;
my $lookingSCopyNumberItems = 0 ; my $lookingRearrangements=0; my $lookingRearrangementsItems=0;
my $lookingBiomarkers =0; my $lookingBiomarkersItems=0;
my $lookingNonHuman =0 ; my $lookingNonHumanItems =0;
my $reportID; my $mrn; my $firstName; my $lastName; my $npi;
my $gender; my $dob; my $dx; my $ordProv; my $recDate; my $recDateLDS;
my $testName; my $summAltCount; my $tumorSite; my $specimenFormat;
my $svariant; my $sgene;  my %geneVariants=();
my %geneVarVPs=(); my $geneVP; my $varVP; my $commonRow; my $strstrand;
my $geneS; my %transcriptS=(); my %cdsS; my $cds;
my %positionS=(); my %strandS=();
my %typeS=(); my %cnS=(); my $copynumber; my %positionCN=();
my %rearrangement=(); my %nonhuman=();
my $type; my $position; my $transcript; my $isVus; my $strand;
my $gotVariants = 0; my $gotCopyNumber = 0; my $gotVarProps = 0;
my $gotRearrangements =0; my $gotBiomarkers =0; my $gotNonHuman=0;
my $svdepth; my $svpcntreads; my $svallelefraction; my $svproteineffect;
my $svfunctionaleffect; my $cnstatus;
my $cnexons; my $cnposition; my $cnratio;
my $cnseglength; my $reardesc; my $rearinframe;
my $rearpos1; my $rearpos2; my $rearuppreadpairs;
my $reartargetgene; my $reartype;
my $biomarkrmicrosat; my $biomarkrTMB; my $biomarkerTMBscore;
my $biomarkrTMBunit; my $nonhumanorganism; my $nonhumanreadspermil;
my $nonhumanstatus; my $personPk; my $obsPk; my $providerPK;
my $yob; my $mob; my $dayob; my $dobdttm;

my $t = localtime;     # what is now?
my $moddttm = $t->strftime("%Y-%m-%d %H:%M:%S");
my $moddt = $t->strftime("%Y-%m-%d");
$moddt = $moddt . ' 00:00:00';
my $obsContext = "FOUNDATION MEDICINE XMLREPORT (OMOP OBSERVATION)";
my %phash=(); my $key; my @tmp;

## scan XMLs
opendir(DIR,".") or die "$!";
my @xmlfiles = grep(/\w+\.xml/, readdir(DIR));
closedir(DIR);
## open output files
open(PERSON, ">GENEPROFILE_PERSON.dat") or die "couldt write to file\n";
print PERSON "IDENTITY_CONTEXT|SOURCE_PK|PERSON_ID|GENDER_CONCEPT_ID|YEAR_OF_BIRTH|MONTH_OF_BIRTH|DAY_OF_BIRTH|BIRTH_DATETIME|DEATH_DATETIME|RACE_CONCEPT_ID|ETHNICITY_CONCEPT_ID|LOCATION_ID|PROVIDER_ID|CARE_SITE_ID|PERSON_SOURCE_VALUE|GENDER_SOURCE_VALUE|GENDER_SOURCE_CONCEPT_ID|RACE_SOURCE_VALUE|RACE_SOURCE_CONCEPT_ID|ETHNICITY_SOURCE_VALUE|ETHNICITY_SOURCE_CONCEPT_ID|MRN|Modified_DtTm\n";

open(PROV, ">GENEPROFILE_PROVIDER.dat") or die 'Coulnt open outfile for provider.dat ';
print PROV "IDENTITY_CONTEXT|SOURCE_PK|PROVIDER_ID|PROVIDER_NAME|NPI|DEA|SPECIALTY_CONCEPT_ID|CARE_SITE_ID|YEAR_OF_BIRTH|GENDER_CONCEPT_ID|PROVIDER_SOURCE_VALUE|SPECIALTY_SOURCE_VALUE|SPECIALTY_SOURCE_CONCEPT_ID|GENDER_SOURCE_VALUE|GENDER_SOURCE_CONCEPT_ID|modified_dtTm\n";

open(OBS,">GENEPROFILE_OBSERVATION.dat") or die 'couldnt open observation outfile';
print OBS  "IDENTITY_CONTEXT|SOURCE_PK|OBSERVATION_ID|RECORD_TYPE|PERSON_ID|OBSERVATION_CONCEPT_ID|OBSERVATION_DATE|OBSERVATION_DATETIME|OBSERVATION_TYPE_CONCEPT_ID|VALUE_AS_NUMBER|VALUE_AS_STRING|VALUE_AS_CONCEPT_ID|QUALIFIER_CONCEPT_ID|UNIT_CONCEPT_ID|PROVIDER_ID|VISIT_OCCURRENCE_ID|VISIT_DETAIL_ID|OBSERVATION_SOURCE_VALUE|UNIT_SOURCE_VALUE|QUALIFIER_SOURCE_VALUE|VARIANT_PROPERTY_NAME|VARIANT_PROPERTY_ISVUS|SHORT_VARIANT_CDS|SHORT_VARIANT_TRANSCRIPT|SHORT_VARIANT_STRAND|SHORT_VARIANT_POSITION|SHORT_VARIANT_DEPTH|SHORT_VARIANT_PERCENTREADS|SHORT_VARIANT_ALLELEFRACTION|SHORT_VARIANT_PROTEINEFFECT|SHORT_VARIANT_FUNCTIONALEFFECT|COPY_NUMBER_ALTERATIONS|COPY_NUMBER_POSITION|COPY_NUMBER_TYPE|COPY_NUMBER_STATUS|COPY_NUMBER_NUMBEREXONS|COPY_NUMBER_RATIO|REARRANGE_DESCRIPTION|REARRANGE_INFRAME|REARRANGE_POS1|REARRANGE_POS2|REARRANGE_SUPPORTREADPAIRS|REARRANGE_TYPE|NON_HUMAN_ORGANISM|NON_HUMAN_READS|NON_HUMAN_STATUS|BIOMARKER_MICROSAT_STABILITY|BIOMARKER_TMB_STATUS|BIOMARKER_TMB_SCORE|TUMOR_SITE|SPECIMEN_TYPE|SUBMITTED_DIAGNOSIS|TEST_NAME|Modified_DtTm\n";

open(FAILS,">FMI_Problems.csv") or die "Couldnt open exceptions file\n";

## loop over input files
foreach $filename (@xmlfiles){
  # reset semaphore flags
  $lookingNPI=1;
  $lookingReportID=0;  $lookingMRN = 0;  $lookingTestType = 0;
  $lookingFirstName = 0;  $lookingLastName = 0; $lookingRecDate = 0;
  $lookingGender = 0;  $lookingDOB = 0; $lookingOrderingMD = 0;  ;
  $lookingOrderingMDid =0;  $lookingSpecSite = 0;   $lookingSpecFormat = 0;
  $lookingSubDX = 0;  $lookingSummCounts = 0; $lookingSCopyNumber = 0;
  $lookingSGenes = 0; $lookingVarProps = 0; $lookingGeneS = 0;
  $lookingVarProperties = 0 ; $lookingSCopyNumberItems = 0 ;
  $gotVariants = 0;  $gotCopyNumber = 0;  $gotVarProps = 0;
  $gotBiomarkers = 0; $gotRearrangements=0; $gotNonHuman=0;
  ## Extract data from XML
  open(F,$filename) or die "counlt open $filename \n";
  LINE:
  while($line = <F>){
    ##print "LINE: $line";
    if($lookingNPI && $line=~/<rr:NPI>(\d+)/){
      $npi = $1;
      $lookingReportID=1; $lookingNPI=0;
      next LINE;
    }elsif($lookingNPI && $line =~/<FinalReport/){
      print FAILS "No NPI on file $filename\n";
      $npi = 'Not Found';
      $lookingReportID=1; $lookingNPI=0;
      next LINE;
    }
    if($lookingReportID && $line=~/<ReportId>/){
      $line = $';
      $line =~ /<\/ReportId>/;
      $reportID = $`;
      $lookingTestType = 1; $lookingReportID = 0;
      next LINE;
    } ## put elsif when element NOT mandatory, avoid running EOF.
    if ($lookingTestType && $line =~ /<TestType>/){
      $line = $';
      $line =~ /<\/TestType>/;
      $testName = $`;
      $lookingTestType = 0; $lookingSpecFormat =1 ;
      next LINE;
    }elsif ($lookingTestType && ( $line =~ /<TestType\/>/ || $line =~ /<TestType \/>/ || $line =~ /<SpecFormat/  )   ){
      $lookingTestType = 0; $lookingSpecFormat =1 ;
      $testName = 'Unknown';
    }
    if ($lookingSpecFormat  && $line =~/<SpecFormat>/){
      $line = $';
      $line =~ /<\/SpecFormat>/;
      $specimenFormat = $`;
      $lookingSpecFormat = 0; $lookingMRN =1 ;
    }
    if ($lookingMRN && $line =~ /<MRN>(\w+)<\/MRN>/){
      $mrn = $1;
      $lookingMRN = 0; $lookingFirstName = 1;
      next LINE;
    }elsif ($lookingMRN && ( $line =~ /<MRN\/>/ || $line =~ /<MRN \/>/ ) ){
      undef $mrn;
      $lookingMRN = 0; $lookingFirstName = 1;
      next LINE;
    }
    if ($lookingFirstName && $line =~ /<FirstName>/){
      $line = $';
      $line =~ /<\/FirstName>/;
      $firstName = $`;
      $lookingFirstName = 0; $lookingLastName =1 ;
      next LINE;
    }elsif($lookingFirstName && $line =~ /<FirstName\/>/){
      undef $firstName;
      $lookingFirstName = 0; $lookingLastName =1 ;
      next LINE;
    }
    if ($lookingLastName && $line =~ /<LastName>/){
      $line = $';
      $line =~ /<\/LastName>/;
      $lastName = $`;
      $lookingLastName = 0; $lookingSubDX =1 ;
      next LINE;
    }elsif($lookingFirstName && $line =~ /<LastName\/>/){
      undef $lastName;
      $lookingLastName = 0; $lookingSubDX =1 ;
      next LINE;
    }
    if ($lookingSubDX && $line =~ /<SubmittedDiagnosis>/){
      $line = $';
      $line =~ /<\/SubmittedDiagnosis>/;
      $dx = $`;
      $lookingSubDX = 0; $lookingGender =1 ;
      next LINE;
    }elsif($lookingSubDX && $line =~ /<SubmittedDiagnosis\/>/){
      undef $dx;
      $lookingSubDX = 0; $lookingGender =1 ;
      next LINE;
    }
    if ($lookingGender && $line =~ /<Gender>/){
      $line = $';
      $line =~ /<\/Gender>/;
      $gender = $`;
      $lookingGender = 0; $lookingDOB =1 ;
      next LINE;
    }elsif($lookingGender && $line =~ /<Gender\/>/){
      undef $gender;
      $lookingGender = 0; $lookingDOB =1 ;
      next LINE;
    }
    if ($lookingDOB && $line =~ /<DOB>/){
      $line = $';
      $line =~ /<\/DOB>/;
      $dob = $`;
      $lookingDOB = 0; $lookingOrderingMD =1 ;
      next LINE;
    }elsif($lookingDOB && $line =~ /<DOB\/>/){
      undef $dob;
      $lookingDOB = 0; $lookingOrderingMD =1 ;
      next LINE;
    }
    if ($lookingOrderingMD && $line =~ /<OrderingMD>/){
      $line = $';
      $line =~ /<\/OrderingMD>/;
      $ordProv = $`;
      $lookingOrderingMD = 0; $lookingOrderingMDid =1 ;
      next LINE;
    }elsif($lookingOrderingMD && $line =~ /<OrderingMD\/>/){
      undef $ordProv;
      $lookingOrderingMD = 0; $lookingOrderingMDid =1 ;
      next LINE;
    }
    if ($lookingOrderingMDid && $line =~ /<OrderingMDId>/){
      $line = $';
      $line =~ /<\/OrderingMDId>/;
      $providerPK = $`;
      $lookingOrderingMDid = 0; $lookingSpecSite =1 ;
      next LINE;
    }elsif($lookingOrderingMDid && $line =~ /<OrderingMDId\/>/){
      undef $providerPK;
      $lookingOrderingMDid = 0; $lookingSpecSite =1 ;
      next LINE;
    }
    if ($lookingSpecSite && $line =~ /<SpecSite>/){
      $line = $';
      $line =~ /<\/SpecSite>/;
      $tumorSite = $`;
      $lookingSpecSite = 0; $lookingRecDate =1 ;
      next LINE;
    }elsif($lookingSpecSite && $line =~ /<SpecSite\/>/){
      undef $tumorSite;
      $lookingSpecSite = 0; $lookingRecDate =1 ;
      next LINE;
    }
    if ($lookingRecDate && $line =~ /<ReceivedDate>/){
      $line = $';
      $line =~ /<\/ReceivedDate>/;
      $recDate = $`;
      $lookingRecDate = 0; $lookingSummCounts =1 ;
      next LINE;
    }elsif($lookingRecDate && $line =~ /<ReceivedDate\/>/){
      undef $recDate;
      $lookingRecDate = 0; $lookingSummCounts =1 ;
      next LINE;
    }
    ## Summaries are independent of gene.
    if($lookingSummCounts && $line =~ /<Summaries alterationCount=\"(\d+)/){
      $summAltCount = $1;
      $lookingSummCounts = 0; $lookingVarProps = 1;
      next LINE;
    }
    if($lookingVarProps && $line =~ /<VariantProperties>/) {
      $lookingVarProps = 0; $lookingVarProperties = 1;
    }elsif($lookingVarProps && $line =~ /<VariantProperties\/>/){
      ## empty <VariantProperties> group - skip, look for <Genes>
      $lookingVarProps = 0; $lookingSGenes = 1;
      $gotVarProps = 0; $lookingVarProperties = 0;
      next LINE;
    }elsif($lookingVarProperties && $line =~ /<VariantProperty geneName=\"(\w+)/){
      $geneVP = $1;
      $line = $';
      $line =~ /isVUS=\"(\w+)/;
      $isVus = $1;
      $line = $';
      $line =~ /variantName=\"(\w+)/;
      $varVP = $1;
      $geneVarVPs{$geneVP} = $varVP;
      $geneVariants{$geneVP}{'vp_isVUS'}= $isVus;
      $geneVariants{$geneVP}{'vp_variantName'} = $varVP;
      next LINE;
    }elsif($lookingVarProperties && $line =~ /<\/VariantProperties/){
      $lookingVarProperties = 0; $lookingSGenes = 1;
      next LINE;
    }
    ##
    ##  We are not parsing the <Genes> block -- seems to have properties alterationProperty:
    ##  isEquivocal, isSubclonal, (alteration)Name.
    ##  Instead, lets' parse subreport: short-variants.
    ##
    if ($lookingSGenes && $line =~/<short-variant\s/){
      $line = $';
      $line =~ /allele-fraction=\"0\.(\d+)/;
      $svallelefraction = '0.'.$1;
      $line = $';
      if( $line =~ /cds-effect=\"(\w+)&gt;(\w*)/  ){
        $cds = $1.'>'.$2;
      }elsif( $line =~ /cds-effect=\"(\w+)_(\w*)/  ){
        $cds = $1.'_'.$2;
      }
      $line = $';
      $line =~ /depth=\"(\d+)/;
      $svdepth = $1;
      $line = $';
      $line =~ /functional-effect=\"(\w+)/;
      $svfunctionaleffect = $1;
      $line = $';
      $line =~ /gene=\"(\w+)/;
      $geneS = $1;
      $line = $';
      $line =~ /percent-reads=\"(\d+)\.(\d+)/;
      $svpcntreads = $1.'.'.$2;
      $line = $';
      $line =~ /position=\"(\w+):(\d+)/;
      $position = $1.':'.$2;
      $line = $';
      $line =~ /protein-effect=\"(\w+)/;
      $svproteineffect = $1;
      $line = $';
      $cdsS{$geneS} = $cds;
      $positionS{$geneS} = $position;

      $line =~ /strand=\"([+-])/;
      $strand = $1;
      $strandS{$geneS} = $strand;
      $line = $';
      $line =~ /transcript=\"NM_(\d+)/;
      $transcript = 'NM_'.$1;
      $transcriptS{$geneS} = $transcript;

      $geneVariants{$geneS}{'shortVariant_cds'} = $cds;
      $geneVariants{$geneS}{'shortVariant_position'} = $position;
      $geneVariants{$geneS}{'shortVariant_strand'} = $strand;
      $geneVariants{$geneS}{'shortVariant_transcript'} = $transcript;
      $geneVariants{$geneS}{'shortVariant_depth'} = $svdepth;
      $geneVariants{$geneS}{'shortVariant_percentreads'} = $svpcntreads;
      $geneVariants{$geneS}{'shortVariant_allelefraction'} = $svallelefraction;
      $geneVariants{$geneS}{'shortVariant_functionaleffect'} = $svfunctionaleffect;
      $geneVariants{$geneS}{'shortVariant_proteineffect'} = $svproteineffect;
      $gotVariants = 1;
      next LINE;
    }elsif($lookingSGenes && $line =~/<\/short-variants/){
      $lookingSGenes = 0; $lookingSCopyNumber = 1;
      next LINE;
    }
    if($lookingSCopyNumber && $line =~/<copy-number-alterations>/) {
      $lookingSCopyNumber = 0;
      $lookingSCopyNumberItems = 1;
      next LINE;
    }elsif($lookingSCopyNumber && $line=~/<copy-number-alterations\/>/) {
      $lookingSCopyNumber = 0;
      $lookingSCopyNumberItems = 0;
      $gotCopyNumber = 0;
      $lookingRearrangements=1;
      next LINE;
    }
    if($lookingSCopyNumberItems && $line =~/<copy-number-alteration\s/){
      $line = $';
      $line =~ /copy-number=\"(\d+)/;
      $copynumber = $1;
      $line = $';
      $line =~ /gene=\"(\w+)/;
      $geneS = $1;
      $line = $';
      $line =~ /number-of-exons=\"(\d+) of (\d+)/;
      $cnexons = $1;
      $line = $';
      $line =~ /position=\"(\w+):(\d+)-(\d+)/;
      $cnS{$geneS} = $copynumber;
      $position = $1.':'.$2.'-'.$3;
      $positionCN{$geneS} = $position;
      $line = $';
      $line =~ /ratio=\"(\d+)\.(\d+)/;
      $cnratio = $1.'.'.$2;
      $line = $';
      $line =~ /status=\"(\w+)/;
      $cnstatus = $1;
      $line = $';
      $line =~ /type=\"(\w+)/;
      $type = $1;
      $typeS{$geneS} = $type;
       $line = $';
      ##segment lenght not seen.
      $geneVariants{$geneS}{'copynumber_cn'} = $copynumber;
      $geneVariants{$geneS}{'copynumber_position'} = $position;
      $geneVariants{$geneS}{'copynumber_type'} = $type;
      $geneVariants{$geneS}{'copynumber_numberexons'} = $cnexons;
      $geneVariants{$geneS}{'copynumber_ratio'} = $cnratio;
      $geneVariants{$geneS}{'copynumber_status'} = $cnstatus;
      $gotCopyNumber = 1;
      next LINE;
    }elsif( $lookingSCopyNumberItems  && $line =~/<\/copy-number-alterations>/){
      $lookingSCopyNumberItems =0; $lookingRearrangements=1;
      next LINE;
    }

    if($lookingRearrangements && $line =~/<rearrangements>/){
       $lookingRearrangements = 0;
       $lookingRearrangementsItems = 1;
       next LINE;
    }elsif($lookingRearrangements && $line=~/<rearrangements\/>/) {
      $lookingRearrangements = 0;
      $lookingRearrangementsItems = 0;
      $gotRearrangements = 0;
      $lookingBiomarkers=1;
      next LINE;
    }
    if($lookingRearrangementsItems && $line =~/<rearrangement\s/){
      $line = $';
#      $line =~ /description=\"(.+)\"/;
      $line =~ /description=\"/;
      $line = $';
      $line =~/\"/; # try to get to the first instance of quotes.
      $reardesc = $`;
      $line = $';
      $line =~ /in-frame=\"(\w+)/;
      $line = $';
      $rearinframe = $1;
      $line =~ /pos1=\"(\w+):(\d+)-(\d+)/;
      $line = $';
      $rearpos1 = $1.':'.$2.'-'.$3;
#      $line =~ /pos2=\"(.+)\"/;
      $line =~ /pos2=\"(\w+):(\d+)-(\d+)/;
      $line = $';
      $rearpos2 = $1.':'.$2.'-'.$3;
      $line =~ /supporting-read-pairs=\"(\d+)/;
      $line = $';
      $rearuppreadpairs  = $1;
      $line =~ /targeted-gene=\"(\w+)/;
      $line = $';
##      $reartargetgene  = $1;
      $geneS = $1;
      $line =~ /type=\"(\w+)/;
      $line = $';
      $reartype  = $1;
      $geneVariants{$geneS}{'rearrange-desc'} = $reardesc;  ## the genename should be the same,
      $geneVariants{$geneS}{'rearrange-inframe'} = $rearinframe;
      $geneVariants{$geneS}{'rearrange-pos1'} = $rearpos1;
      $geneVariants{$geneS}{'rearrange-pos2'} = $rearpos2;
      $geneVariants{$geneS}{'rearrange-supportreadpairs'} = $rearuppreadpairs;
      $geneVariants{$geneS}{'rearrange-type'} = $reartype;
      $gotRearrangements=1;
    }elsif($lookingRearrangementsItems && $line=~/<\/rearrangements>/) {
       $lookingRearrangementsItems=0; $lookingBiomarkers=1;
    }
    if($lookingBiomarkers && $line =~/<biomarkers>/){
       $lookingBiomarkers = 0;
       $lookingBiomarkersItems = 1;
       next LINE;
    }elsif($lookingBiomarkers && $line=~/<biomarkers\/>/) {
      $lookingBiomarkers = 0;
      $lookingBiomarkersItems = 0;
      $gotBiomarkers = 0;
      $lookingNonHuman=1;
      next LINE;
    }
    if($lookingBiomarkersItems && $line =~/<microsatellite/){
       $line =~/status=\"(\w+)/;
       $biomarkrmicrosat = $1;
       next LINE;
    }elsif($lookingBiomarkersItems && $line =~/<tumor/){
       $line =~/score=\"(\d+)\.(\d+)/;
       $line = $';
       $biomarkerTMBscore = $1.'.'.$2;
       $line =~/status=\"(\w+)/;
       $biomarkrTMB = $1;
       $biomarkrTMBunit = 'mutations per megabase'; ## hardcoded, seems always this.
       next LINE;
    }elsif($lookingBiomarkersItems && $line=~/<\/biomarkers>/) {
      $lookingBiomarkers = 0;
      $lookingBiomarkersItems = 0;
      $gotBiomarkers = 1;
      $lookingNonHuman=1;
    }
    if($lookingNonHuman && $line =~/<non-human-content>/){
      $lookingNonHuman = 0;
      $lookingNonHumanItems =1 ;
    }elsif($lookingNonHuman && $line=~/<non-human-content\/>/) {
      $lookingNonHuman = 0;
      $lookingNonHumanItems = 0;
      $gotNonHuman = 0;
      next LINE;
    }
    if($lookingNonHumanItems && $line =~/organism=\"/){
       $line=$';
       $line =~/\"/;
       $nonhumanorganism = $`;
       $line = $';
       $line =~/reads-per-million=\"(\d+)/;
       $nonhumanreadspermil = $1;
       $line=$';
       $line =~/status=\"(\w+)/;
       $nonhumanstatus = $1;
       $nonhuman{$nonhumanorganism}{'nonhuman-reads'} = $nonhumanreadspermil;
       $nonhuman{$nonhumanorganism}{'nonhuman-status'} = $nonhumanstatus;
    }
  }
  ## transform some elements, dump data for loads

  $t = localtime;
  $moddttm = $t->strftime("%Y-%m-%d %H:%M:%S");

  $ordProv =~ s/,//;
 ## $personPk = $reportID.$mrn;
  $personPk = $mrn;
  $dob =~ /(\d{4})-(\d{2})-(\d{2})/;  $dayob = $3; $yob = $1; $mob = $2;
  if ($dob =~/(\d+)/){
    $dobdttm = $dob.' 00:00:00';
  }else{
    undef $dobdttm;
  }
  $recDateLDS = $recDate.' 00:00:00';

  print PERSON "FOUNDATIONMEDICINE XMLREPORT (OMOP PERSON)|$personPk|$mrn||$yob|$mob|$dayob|$dobdttm||||LOCID|$providerPK|caresiteid|$mrn|$gender||||||$mrn|$moddt\n";
  print PROV "FOUNDATIONMEDICINE XMLREPORT (OMOP PROVIDER)|$providerPK|$npi|$ordProv|$npi|||caresiteid|||$providerPK|||||$moddt\n";
  ##if($ordProv!=/\w/){
  if(length($ordProv)<2){
    print FAILS "No Provider Name,$reportID \n";
  }
  if(length($mrn)<3){
    print FAILS "Bad MRN,$reportID \n";
  }
  $commonRow = "$mrn, $firstName, $lastName, $gender, $dob, $recDate, $testName, $dx, $tumorSite, $specimenFormat, $ordProv, $reportID, $summAltCount";

  if ($gotVarProps) {
    foreach my $geneVP (keys(%geneVarVPs)) {
      $obsPk = $reportID.'_'.$geneVP.'_'.$geneVarVPs{$geneVP};
      print "$commonRow, $geneVP, $geneVarVPs{$geneVP},,,,,,,, \n";
    }
  }else{
    $obsPk = $reportID;
  }

  if ($gotVariants) {
    foreach my $geneS (keys(%cdsS)) {
      $obsPk = $reportID.'_'.$geneS.'_'.$cdsS{$geneS};
    }
    foreach my $geneS (keys(%transcriptS)) {
      $obsPk = $reportID.'_'.$geneS.'_'.$transcriptS{$geneS};
    }
    foreach my $geneS (keys(%strandS)) {
      if ($strandS{$geneS}=~/\+/){
        $strstrand = 'Positive';
      }elsif($strandS{$geneS}=~/-/){
        $strstrand = 'Negative';
      }
      $obsPk = $reportID.'_'.$geneS.'_'.$strstrand;
    }
    foreach my $geneS (keys(%positionS)) {
      $obsPk = $reportID.'_'.$geneS.'_'.$positionS{$geneS};
      print "$commonRow,,, $geneS,,,, $positionS{$geneS}\n";
    }
  }
  if ($gotCopyNumber) {
    foreach my $geneS (keys(%cnS)) {
        print "$commonRow,,,,,,,, $geneS, $cnS{$geneS},,\n";
        $obsPk = $reportID . '_' . $geneS . '_' . $cnS{$geneS};
    }
    foreach my $geneS (keys(%positionCN)) {
        print "$commonRow,,,,,,,, $geneS, $positionCN{$geneS},,\n";
        $obsPk = $reportID . '_' . $geneS . '_' . $positionCN{$geneS};
    }
    foreach my $geneS (keys(%typeS)) {
        print "$commonRow,,,,,,,, $geneS,,, $typeS{$geneS}\n";
        $obsPk = $reportID . '_' . $geneS . '_' . $typeS{$geneS};
    }
  }

  foreach my $geneS (keys(%geneVariants)) {
    $obsPk =  $reportID.'/'.$geneS.'/'.$geneVariants{$geneS}{'vp_variantName'}.'/'.$geneVariants{$geneS}{'vp_isVUS'}.'/'.$geneVariants{$geneS}{'shortVariant_cds'}.'/'.$geneVariants{$geneS}{'shortVariant_transcript'}.'/'.$geneVariants{$geneS}{'shortVariant_strand'}.'/'.$geneVariants{$geneS}{'shortVariant_position'}.'/'.$geneVariants{$geneS}{'copynumber_cn'}.'/'.$geneVariants{$geneS}{'copynumber_position'}.'/'.$geneVariants{$geneS}{'copynumber_type'};
    print  "$obsContext|$obsPk|$obsPk|$personPk||$recDateLDS|$recDateLDS|Somatic Genetic Profile||$geneS||||$providerPK|||||||||||||||$typeS{$geneS}|$tumorSite|$specimenFormat|$dx|$testName|$moddttm\n";
    print OBS "$obsContext|$obsPk|$obsPk|RECORD_ALTERATION_PROPERTY|$personPk||$recDateLDS|$recDateLDS|Somatic Genetic Profile|";
    print OBS "|$geneS|";
    print OBS "|||$npi||||||$geneVariants{$geneS}{'vp_variantName'}|$geneVariants{$geneS}{'vp_isVUS'}|$geneVariants{$geneS}{'shortVariant_cds'}|$geneVariants{$geneS}{'shortVariant_transcript'}|$geneVariants{$geneS}{'shortVariant_strand'}|";
    print OBS "$geneVariants{$geneS}{'shortVariant_position'}|$geneVariants{$geneS}{'shortVariant_depth'}|$geneVariants{$geneS}{'shortVariant_percentreads'}|";
    print OBS "$geneVariants{$geneS}{'shortVariant_allelefraction'}|$geneVariants{$geneS}{'shortVariant_proteineffect'}|$geneVariants{$geneS}{'shortVariant_functionaleffect'}|";
    print OBS "$geneVariants{$geneS}{'copynumber_cn'}|$geneVariants{$geneS}{'copynumber_position'}|$geneVariants{$geneS}{'copynumber_type'}|";
    print OBS "$geneVariants{$geneS}{'copynumber_status'}|$geneVariants{$geneS}{'copynumber_numberexons'}|$geneVariants{$geneS}{'copynumber_ratio'}|";
    print OBS "$geneVariants{$geneS}{'rearrange-desc'}|$geneVariants{$geneS}{'rearrange-inframe'}|$geneVariants{$geneS}{'rearrange-pos1'}|";
    print OBS "$geneVariants{$geneS}{'rearrange-pos2'}|$geneVariants{$geneS}{'rearrange-supportreadpairs'}|$geneVariants{$geneS}{'rearrange-type'}|";
    print OBS "|||$biomarkrmicrosat|$biomarkrTMB|$biomarkerTMBscore|$tumorSite|$specimenFormat|$dx|$testName|$moddttm\n";
  }

  foreach my $nonhumanorganism (keys(%nonhuman)) {
     $obsPk =  $reportID.'/'.$nonhumanorganism.'/'.$nonhuman{$nonhumanorganism}{'nonhuman-reads'}.'/'.$nonhuman{$nonhumanorganism}{'nonhuman-status'};
     print OBS "$obsContext|$obsPk|$obsPk|RECORD_NON_HUMAN_CONTENT|$personPk||$recDateLDS|$recDateLDS|Somatic Genetic Profile|";
 ##    print OBS "|$nonhumanorganism||||$npi|||||||||||"; ## send organism elsewhere.
     print OBS "|||||$providerPK|||||||||||";
     print OBS "|||";
     print OBS "|||";
     print OBS "|||";
     print OBS "|||";
     print OBS "|||";
     print OBS "|||$nonhumanorganism|";
     print OBS "$nonhuman{$nonhumanorganism}{'nonhuman-reads'}|$nonhuman{$nonhumanorganism}{'nonhuman-status'}|";
     print OBS "$biomarkrmicrosat|$biomarkrTMB|$biomarkerTMBscore|$tumorSite|$specimenFormat|$dx|$testName|$moddttm\n";
  }
  undef %geneVariants;   undef %geneVarVPs; undef %nonhuman;
  undef %strandS; undef %cnS; undef %positionCN; undef %typeS;
  undef %cdsS; undef %transcriptS; undef %positionS;
  undef $reportID; undef $mrn; undef $firstName; undef $lastName; undef $npi;
  undef $gender; undef $dob; undef $dx; undef $ordProv; undef $recDate; undef $recDateLDS;
  undef $testName; undef $summAltCount; undef $tumorSite; undef $specimenFormat;
  undef $svariant; undef $sgene;
  undef $geneVP; undef $varVP; undef $commonRow; undef $strstrand;
  close(F);
}
close(OBS);
close(PROV);
close (FAILS);
close (PERSON);

## lets dedupe the provider list: open list, create hash and dedupe, print list.

open(F, "GENEPROFILE_PROVIDER.dat") or die "Coulnt open outfile for GENEPROFILE_PROVIDER.dat \n";
  while($line = <F>){
     @tmp = split(/\|/,$line);
     $key = $tmp[1]; # the Provider ID, should be unique.
     if($key=~/SOURCE/){next;}
     $phash{$key} = $line;  # Should overwrite the previous key/val pair, thus effectively
                            # deduping the file.
  }
close(F);

open(PROV, ">GENEPROFILE_PROVIDER.dat") or die 'Coulnt ooverwrite file for GENEPROFILE_PROVIDER.dat ';

print PROV "IDENTITY_CONTEXT|SOURCE_PK|PROVIDER_ID|PROVIDER_NAME|NPI|DEA|SPECIALTY_CONCEPT_ID|CARE_SITE_ID|YEAR_OF_BIRTH|GENDER_CONCEPT_ID|PROVIDER_SOURCE_VALUE|SPECIALTY_SOURCE_VALUE|SPECIALTY_SOURCE_CONCEPT_ID|GENDER_SOURCE_VALUE|GENDER_SOURCE_CONCEPT_ID|modified_dtTm\n";

foreach $key (keys %phash) {
  print PROV "$phash{$key}";
}

close(PROV);

undef %phash;

open(F, "GENEPROFILE_PERSON.dat") or die "Coulnt open outfile for GENEPROFILE_PERSON.dat \n";
while($line = <F>){
  @tmp = split(/\|/,$line);
  $key = $tmp[1]; # the Person ID, should be unique.
  if($key=~/SOURCE/){next;}
  $phash{$key} = $line;  # Should overwrite the previous key/val pair, thus effectively
                            # deduping the file.
}
close(F);

open(PAT, ">GENEPROFILE_PERSON.dat") or die 'Coulnt ooverwrite file for GENEPROFILE_PERSON.dat ';

print PAT "IDENTITY_CONTEXT|SOURCE_PK|PERSON_ID|GENDER_CONCEPT_ID|YEAR_OF_BIRTH|MONTH_OF_BIRTH|DAY_OF_BIRTH|BIRTH_DATETIME|DEATH_DATETIME|RACE_CONCEPT_ID|ETHNICITY_CONCEPT_ID|LOCATION_ID|PROVIDER_ID|CARE_SITE_ID|PERSON_SOURCE_VALUE|GENDER_SOURCE_VALUE|GENDER_SOURCE_CONCEPT_ID|RACE_SOURCE_VALUE|RACE_SOURCE_CONCEPT_ID|ETHNICITY_SOURCE_VALUE|ETHNICITY_SOURCE_CONCEPT_ID|MRN|Modified_DtTm\n";

foreach $key (keys %phash) {
  print PAT "$phash{$key}";
}

close(PAT);
