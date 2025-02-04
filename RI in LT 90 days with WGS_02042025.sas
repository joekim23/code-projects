***********************************************************************
*	Title: Short RI Analysis Code								   	  *
*	Description: (1) Create line level all SARS-CoV-2 events      	  *
*				 in NYC where the specimen collection date of         *
*				 both the initial sequenced test and the          	  *
*				 subsequently sequenced test(s) falls into the    	  *
*				 period of 1/1/2021-12/31/2023 will be included.  	  *
*				 												  	  *
*																  	  *
*	Created by:  Allegra Wilson, Joe Kim						  	  *	
*	Create date: 08/01/2023										  	  *
*																      *
*	Code adapted from a portion of Alison Levin-Rector's 8/2/2021 	  *
*	code written by Libby Luoma, titled "Repeat Positive COVID     	  *
*	cases - PCR and Antigen.										  *
*																	  *
*	Last edited: 12/19/2024										 	  *
*																 	  *
***********************************************************************;

/* Notes:
	- Circa Jun 2021, 90 day rule implemented where reinfections are assigned a 
	new event ID. */	

*set libraries for necessary data; 
libname maven odbc noprompt="SERVER=SQLMAVENBCDRPT1;DRIVER=ODBC Driver 17 for SQL Server;Trusted_Connection=YES; DATABASE=MavenBCD" schema=DBO readbuff=10000 sql_functions=all;
libname cases "\\NASLOCSHARE240\AgencyShare\EmergencyEvents\2019_nCoV\SurvEpi\EpiData Unit Doc Ops\Datasets for analyses\Event data";
libname events "\\NASLOCSHARE240\AgencyShare\EmergencyEvents\2019_nCoV\SurvEpi\EpiData Unit Doc Ops\Datasets for analyses\Event data";
libname poverty "\\NASLOCSHARE240\AgencyShare\Data Task Force\Reference_Tips\Area-based poverty measure\Data\ACS 2018_2022\zip code\ZCTA";
libname cong "\\naslocshare240\agencyshare\EmergencyEvents\2019_nCoV\CSIRU_secure\Congregate Facility Lists";
libname hosp "\\NASLOCSHARE240\Agencyshare\EmergencyEvents\2019_nCoV\SurvEpi\EpiData Unit Doc Ops\Matching projects\Hospitalization\Clean Hospitalization Dataset\Output Datasets";

*set rules for SAS variable names;
option VALIDVARNAME=V7; 

*************************************************************************************************
*(1) Prep preliminary data set for PHL: Identify individuals with multiple specimens with WGS   *
*************************************************************************************************;

proc contents data= hosp.ncov_hospitalizations;
run;

*select sequenced specimens from PHL and PRL from 01 Jan 2021 - 31 Dec 2023
Things to do:
-calculate how many OCME results there are, depending on number -> 207 exclude (when excluding the result_description)
-calculate how many blank external_id records are, depending on number -> 18928 exclude (when excluding the result_description)
-calculate how many we exclude by removing result_description == 
	not done - 91,893
	indeterminante - 6,030 
	failed, - 0 --> should we actually exclude == None (4,006)?
	unassigned - 693
-calculate how many missing specimen collection dates there are -> none to exclude
-remove variables that aren't being used once we decide to exclude variables
-exclude party_id with only 1 specimen collection date 
-reformat based on proposed template and calculate the difference in specimen dates 
-confirm that all results have been filtered to NYC already 
;

/***********************************************************************************************************************/

/*WIP: EXPERIMENTING WITH PASS THROUGH CODING*/
/*proc sql;
connect using maven;
create table wgs_labs as
	select 	datepart(a.specimen_date) as specimen_date format mmddyy10.	
			, datepart(a.lab_create_date) as lab_create_date format mmddyy10.
        	, case when a.report_date = . then '01JAN1900'd
        	     else datepart(a.report_date) 
        	     end as report_date format mmddyy10.
        	, case when datepart(a.lab_create_date) < '11MAR2021'd  and index(a.ordering_facility_name, 'OCME') >0 then 1 
               	when datepart(a.lab_create_date) >= '11MAR2021'd and index(a.ordering_facility_name, 'OCME') >0 then 0 
               	else . 
				end as ocme_error
			,c.*
	from maven.dd_3_aow_labs_covid a left join connection to maven
		(select b.event_id
			, a.specimen_number
			, a.external_ID	  
			, a.lab_name
			, a.lab_clia
			, a.test_name
			, a.result_description
			, a.ordering_facility_name
		    , b.party_id
		from dbo.dd_3_aow_labs_covid a  left join dbo.dd_3_aow_events_covid b
				on a.event_id=b.event_id
			where '2021-01-01 00:00:00.000' <= specimen_date and specimen_date <= '2023-12-31 00:00:00.000'
				and a.test_name = 'WGS-COVID'
				and UPPER (a.result_description) != 'NOT DONE' 
				and UPPER (a.result_description) != 'FAILED'
				and UPPER (a.result_description) != 'NONE'
           		and a.result_description != 'Indeterminate' 
           		and a.result_description != 'Unassigned' 
				and (a.lab_clia = '33D0679872' or a.lab_clia ='31D0106549')) as c
	on a.event_id=c.event_id;
quit; run;*/

/*****************************************************************************************************************/

proc sql;
	create table wgs_labs as
		select  a.event_id 
				, datepart(a.specimen_date) as specimen_date format mmddyy10.	
				, datepart(a.lab_create_date) as lab_create_date format mmddyy10.
        		, case when a.report_date = . then '01JAN1900'd
        	 	    else datepart(a.report_date) 
        	   	    end as report_date format mmddyy10.
				, report_status
        		, case when datepart(a.lab_create_date) < '11MAR2021'd  and index(a.ordering_facility_name, 'OCME') >0 then 1 
            	   	when datepart(a.lab_create_date) >= '11MAR2021'd and index(a.ordering_facility_name, 'OCME') >0 then 0 
            	   	else . 
				end as ocme_error
				, a.specimen_number
				, a.external_ID	  
				/*PHL accession number?*/ /*May just be specimen number (nearly identical to external_ID*/
				, a.lab_name
				, a.lab_clia
				, a.test_name
				, a.result_description
				, a.ordering_facility_name
		    	, b.party_id
		from maven.dd_3_aow_labs_covid a left join cases.ncov_all_events b
			on a.event_id = b.event_id
		where '01JAN2021'd <= specimen_date and specimen_date <= '31DEC2023'd
				and a.test_name = 'WGS-COVID'
				and a.report_status = 'corrective'
				and upcase(a.result_description) ^= 'NOT DONE' 
				and upcase(a.result_description) ^= 'FAILED'
				and upcase(a.result_description) ^= 'NONE'
           		and a.result_description ^= 'Indeterminate' 
           		and a.result_description ^= 'Unassigned' 
				and (a.lab_clia = '33D0679872' or a.lab_clia ='31D0106549');
		quit;

data wgs_labs_clean;
set wgs_labs;
if ocme_error = 1 then delete; /*remove tests that don't have an accession number since PHL needs that or OCME error = 1*/
run;

proc sql;
create table wgs_labs_raw as
	select distinct
		party_id,
		event_id,
		external_ID, /*aka accession number*/
		specimen_number, /*PHL accession number?*/
		specimen_date,
		report_date,
/*		report_status,*/
		result_description,
		lab_name,
		ordering_facility_name
/*		count(party_id) as n_party_id,*/
/*		count(distinct external_id) as total_wgs_labs*/
	from wgs_labs_clean
	group by party_id, event_id, external_id
	having report_date = max(report_date);
;quit;
data wgs_step2_singles wgs_step2_dups; 
	set wgs_labs_raw; 
	by party_id; 
	if first.party_id and last.party_id then output wgs_step2_singles; 
		else output wgs_step2_dups;
run; 


/*/*Create wider wgs labs table including different wgs results to take a look*/*/
/*proc sql;*/
/*create table wgs_labs*/
/*as select * from wgs_labs_raw*/
/*order by party_id asc, specimen_date asc;*/
/*run;*/
/**/
/*proc transpose data=wgs_labs out=wgs_labs_wide (drop= _name_ _label_) prefix = result_desc;*/
/*by party_id;*/
/*var result_description;*/
/*run;*/
; 

proc sql;
create table party_ids_dups
as select distinct * 
from wgs_step2_dups;
quit;
/*JK: Per discussion with Faten, keeping all dyads regardless of variant difference to investigate potential differences in sublineage*/

/*Inspect*/
/*proc sql;
create table mytest as
select * from wgs_labs_wide a inner join party_ids_diff_vars b on
a.party_id = b.party_id;
run;*/

/*Calculate difference in days between reinfections*/
proc sql;
create table wgs_step3 as
select distinct party_id, event_id, 
min(specimen_date) as init_date format date9.,
max(specimen_date) as ri_date format date9.,
max(specimen_date)-min(specimen_date) as diff_days /*Difference in days between the most recent lab and the earliest lab (may change if multiple reinfections for a given individual*/
from wgs_step2_dups 
where party_id in (select party_id from party_ids_dups)
group by party_id;
run;

data wgs_labs_final; set wgs_step3;
where diff_days<90;
run;

data wgs_labs_final; set wgs_labs_final (drop= init_date ri_date);
run;

proc sort data= wgs_labs_final;
by party_id;
run;

*************************************************************************************************
*(2) Import PHL reviewed dataset and match with variables of interest for analysis 				*
*************************************************************************************************;
/*JK: To stay here or to continue in separate code?*/ 
/*Per Rebecca's suggestion, review of PHL's returned file will be in separate code. 

/***Continuing preliminary analysis below***/
proc sql; 
	create table hbptemp as	
		select *
		from wgs_step2_dups
		where party_id in (select party_id from wgs_labs_final); 
quit; 

/*Transposing wgs data so that each row is at the individual level and not lab level*/
proc transpose data= hbptemp out= wgs_external_id (drop= _name_ _label_) prefix= external_ID;
by party_id;
var external_id ;
run;

proc sort data=hbptemp out=hbptemp_2 nodupkey; 
	by party_id event_id result_description; 
run; 
proc transpose data= hbptemp_2 out= wgs_result (drop= _name_ _label_) prefix= result;
by party_id;
var result_description;
run;

proc transpose data= hbptemp out= wgs_spec_dt (drop= _name_) prefix = specimen_date;
by party_id;
var specimen_date;
run; 

proc transpose data= hbptemp out= wgs_report_dt (drop= _name_) prefix = report_date;
by party_id;
var report_date;
run;

proc transpose data= hbptemp out= wgs_lab (drop= _name_ _label_) prefix= lab_name;
by party_id;
var lab_name;
run;

data wgs_labs_prelim;
merge wgs_external_id wgs_spec_dt wgs_report_dt wgs_lab wgs_result wgs_step3;
by party_id;
run;

/*Filter out dyads with intervals at least 90 days apart, and identify dyads with distinct variants*/
data wgs_labs_prelim; set wgs_labs_prelim;
	if result2='' and result3='' then diffvariant=0; 
		else if result1 ne result2 then diffvariant=1; 
		else if result3 ne '' and result1 ne result3 then diffvariant=1; 
		else if result3 ne '' and result2 ne result3 then diffvariant=1; 
		else diffvariant=0; 
run;

data wgs_labs_prelim; set wgs_labs_prelim;
		where 9<diff_days<90; 
/*		where diffvariant=1; */
run;


/*Checks*/
proc freq data=wgs_labs_prelim; 
	table diff_days; 
run; 

proc freq data=wgs_labs_prelim; 
	table diffvariant*result1*result2*result3 / list missing nopercent nocum; 
run; 

proc sql; 
	select diff_days, count(*)
	from wgs_labs_prelim
	where diffvariant=1
	group by diff_days; 
quit; 

proc freq data= wgs_labs_prelim;
table diff_days;
where diffvariant =1;
run;

*HBP stopped here; 


/*Matching with demographic variables from events dataset*/
proc sql;
	create table ri_analysis_raw as	
		select a.party_id,
			   a.event_id,
			   b.age,
			   b.gender,
			   b.race_ethnicity,
			   b.boro,
			   b.date_of_death as dod format date8.,
			   case when b.date_of_death ^=. then 'Yes'
			   		else 'No' end as death,
			   b.diagnosis_date,
			   b.disease_status_final,
			   input(b.zip, best.) as zip,
			   b.bin,
			   a.diff_days
	from wgs_labs_prelim a left join cases.ncov_all_events b
		on a.event_id = b.event_id
		and b.disease_status_final in ('CONFIRMED', 'PROBABLE');
quit;

/*Add hospitalization status (only when a hospitalization occurred after the second infection)*/
proc sql;
	create table ri_analysis_hosp as
		select distinct a.*,
			   max(a.specimen_date) as last_spec_date format date10.,
			   b.date_hospitalized,
			   case when max(a.specimen_date) < b.date_hospitalized and b.date_hospitalized ^=. then 'Yes'
			   		else 'No' end as hosp,
				max(specimen_date)-min(specimen_date) as diff_days
	from hbptemp a left join hosp.ncov_hospitalizations b
		on a.event_id = b.event_id
		group by party_id;
quit;

data ri_analysis_hosp; set ri_analysis_hosp;
if hosp = 'Yes';
run;


proc sql; 
	create table ri_analysis_raw_merge as 
		select distinct a.*,
			   case when b.hosp ^= 'Yes' then 'No'
			   		end as hosp
	from ri_analysis_raw a left join ri_analysis_hosp b
		on a.party_id = b.party_id;
quit;

			   
;
proc contents data= hosp.ncov_hospitalizations;
run;


/*Import neighborhood poverty data*/
data poverty; set poverty.zcpov1822;
format pov_level $10.;
if povgroup4_1822 = 1 then pov_level = 'Low';
	else if povgroup4_1822 = 2 then pov_level = 'Medium';
	else if povgroup4_1822 = 3 then pov_level = 'High';	
	else if povgroup4_1822 = 4 then pov_level = 'Very High';
run;

/*Import congregate settings data*/
proc sql;
	create table cong_flag as
		select distinct bin as cong_bin, business_type
	from maven.dd_exposure_analysis
		     where business_type in
				( 'Developmental Disabilities Facility', 'Shelter', 'Respite Center', 'HERRC', 'LTC Facility', 'Assisted Living Facility (non-Art. 28)', 'Prison/Jail', 'Mental health residential facility',
				  'Group home', 'Supportive housing', 'Substance use treatment residential facility', 'Independent senior living facility', 'Mental health care', 'Transitional housing', 'Children/Adolescent residential facility',
				  'Other Congregate Living Facility');
quit;

/*Merge in the above tables into the analysis data*/
proc sql; 
	create table ri_analysis_pre as	
		select distinct a.*,
						case when a.bin = b.cong_bin then 'Yes' 
							when a.bin = '' then 'No'
							else 'No' end as cong_fac,
						c.pov_level
	from ri_analysis_raw_merge a left join cong_flag b
		on a.bin = b.cong_bin
	left join poverty c
		on a.zip = c.zcta;
/*	where a.bin not in ('1000000','2000000','3000000','4000000','5000000');*/
quit;

/*Format vars and finalize analysis data*/
data ri_analysis; set ri_analysis_pre;
	borough = propcase(boro);
	race_eth = propcase(race_ethnicity);
	gender = propcase(gender);
	format age_group $5.;
	if gender in ('','Unknown') then gender = 'Unknown';
	if borough = 'Nyc Borough Unknown' then delete;
	if race_eth = 'Hispanic/Latino' then race_eth = 'Latino';
	if .< age <18 then age_group = '0-17';
		else if 18 <= age <= 44 then age_group = '18-44';
		else if 45 <= age <= 54 then age_group = '45-54';
		else if 55 <= age <= 64 then age_group = '55-64';
		else if 65 <= age <= 74 then age_group = '65-74';
		else if 75 <= age <= 106 then age_group = '75+';
run;

*************************************************************************************************
*(3) Create Table 1																				*
*************************************************************************************************;

proc sql;
		create table ri_table_1 as

			select distinct '1. Total' as cat, 'Total' as var, count(*) as num					
			from ri_analysis

			union

			select distinct '2. Gender', gender, count(*)
			from ri_analysis
				group by gender

			union 

			select distinct '3. Age Group', age_group, count(*)
			from ri_analysis
				group by age_group

			union

			select distinct '4. Race/Ethnicity', race_eth, count(*)
			from ri_analysis
				group by race_eth

			union

			select distinct '5. Borough', borough, count(*)
			from ri_analysis
				group by borough

			union

			select distinct '6. Congregate Facility', cong_fac, count(*)
			from ri_analysis
				group by cong_fac

			union 

			select distinct '7. Neighborhood Poverty Level', pov_level, count(*)
			from ri_analysis
				where pov_level ^=''
				group by pov_level

			/*vaccination status TBD*/

			union

			select distinct '8. Hospitalization', hosp, count(*)
			from ri_analysis
				group by hosp

			union

			select distinct '9. Death', death, count(*)
			from ri_analysis
				group by death 

			/*union*/

			/*select distinct '9. Divergence type', div_type, count(*)
			from ri_analysis
					group by div_type*/
				

			;quit;

/*Create table shell*/
data ri_shell; 
 	length category $40 Label $150 age_group $100 borough $ 25 ;
	input order category $ label $;
	infile datalines delimiter='|'; 
	datalines;
1|1. Total|Total
1|2. Gender|Male
2|2. Gender|Female
1|3. Age Group|0-17
2|3. Age Group|18-44
3|3. Age Group|45-54
4|3. Age Group|55-64
5|3. Age Group|65-74
6|3. Age Group|75+
1|4. Race/Ethnicity|Latino
2|4. Race/Ethnicity|Black/African American
3|4. Race/Ethnicity|Asian/Pacific Islander
4|4. Race/Ethnicity|White
5|4. Race/Ethnicity|Other
6|4. Race/Ethnicity|Unknown
1|5. Borough|Bronx
2|5. Borough|Brooklyn
3|5. Borough|Manhattan
4|5. Borough|Queens
5|5. Borough|Staten Island
1|6. Congregate Facility|Yes
2|6. Congregate Facility|No 
1|7. Neighborhood Poverty Level|Low
2|7. Neighborhood Poverty Level|Medium
3|7. Neighborhood Poverty Level|High
4|7. Neighborhood Poverty Level|Very High
1|8. Hospitalization|Yes
2|8. Hospitalization|No
1|9. Death|Yes
2|9. Death|No
;
run;

/*Merging data with table shell*/
proc sql; 
	create table table1 (drop= category label order) as
		select  b.category, b.label, b.order,
				a.*
		from ri_table_1 a
		full join ri_shell b
		on a.cat=b.category and a.var=b.label
		order by a.cat, b.order, b.label;
quit; 

proc sql;
	drop table ri_analysis_raw, poverty, cong_flag, ri_analysis_pre, ri_table_1, ri_shell;
run;

*************************************************************************************************
*(4) Calculate the distribution of days in reinfections											*									*
*************************************************************************************************;

proc univariate data= ri_analysis;
var diff_days;
run;

