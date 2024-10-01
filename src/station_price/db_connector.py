from sqlalchemy import create_engine
import pandas as pd
from decouple import config

def get_db_connection():
    """
    Establishes and returns a connection to the SQL database using SQLAlchemy.
    For Windows authentication, the connection string will use pyodbc and the MSSQL driver.
    """
    db_driver = config('DB_DRIVER')
    db_server = config('DB_SERVER')
    db_name = config('DB_NAME')
    
    # Connection string for SQLAlchemy using pyodbc and Windows Authentication
    conn_str = f"mssql+pyodbc://@{db_server}/{db_name}?driver={db_driver}&trusted_connection=yes"
    
    # Create SQLAlchemy engine
    engine = create_engine(conn_str)
    
    return engine

def fetch_data(start_date: str, end_date: str) -> pd.DataFrame:
    """
    Fetches data from the SQL Server database based on the provided date range.
    The SQL query selects fields like SalesAreaNo, DemoNumber, YearMonth, DurationImpacts, RatecardImpacts.
    
    Args:
        start_date (str): The start date in YYYY-MM-DD format.
        end_date (str): The end date in YYYY-MM-DD format.
    
    Returns:
        pd.DataFrame: DataFrame with the query results.
    """
    engine = get_db_connection()

    query = f"""
    With 
	DateComparions As (
		--Adjust these dates to show whatever is required - it'll only work for a given month e.g. June 2024
		SELECT Cast('2024-06-01' As Date) StartDate, Cast('2024-06-30' As Date) EndDate, Cast(202406 As Int) YearMonth )
	,BarbSpotData As (
				SELECT
					[Barb_Area_No]
					,[Barb_Area_Name]
					,[Barb_Area_Desc]
					,[Barb_Station_No]
					,[Barb_Station_Name]
					,[barb_log_station]
					,CASE [sales_house_fkey] WHEN 17 THEN 'GMB' ELSE 'ITV' END as BroadcastSource	--[sales_house_fkey]
					,[sare_no]
					,rc_factor
					,rtg_type
					,spot_no
					,spot_length
					,YearMonth
					--,base_length
					,substring([Demo],5, len([Demo])) as [Demo]
					,[ViewingNumber]
					,cast(sum(cast(ViewingNumber as FLOAT) * (spot_length/30.0)) over  (partition by [demo], [sales_house_fkey],sare_no order by demo, sales_house_fkey, sare_no) as FLOAT) [DurationImpacts]
					,cast(sum(cast(ViewingNumber as FLOAT) * rc_factor) over  (partition by [demo], [sales_house_fkey],sare_no order by demo, sales_house_fkey,sare_no) as FLOAT) [RatecardFactorImpacts]
				FROM
					(SELECT
						[Barb_Area_No]
						,[Barb_Area_Name]
						,[Barb_Area_Desc]
						,[Barb_Station_No]
						,[Barb_Station_Name]
						,[barb_log_station]
						,[sales_house_fkey] 
						,bc.[sare_no]
						,rc_factor
						,rtg_type
						,spot_no
						,spot_length
						--,base_length
						,[qty_homes]
						,[qty_hwives]
						,[qty_hwschd]
						,[qty_hw1654]
						,[qty_hwabc1]
						,[qty_adults]
						,[qty_ad1624]
						,[qty_ad1634]
						,[qty_ad1654]
						,[qty_adabc1]
						,[qty_men]
						,[qty_me1634]
						,[qty_meabc1]
						,[qty_women]
						,[qty_wo1634]
						,[qty_woabc1]
						,[qty_childs]
						,[qty_chil49]
						,dtc.YearMonth
					FROM	--select top 10 * from
						[ITV_ODS].[AtsDwDbViews].[uvw_fct_Barb_Spot_Augmented_StationPrices] as c (NOLOCK)		--ex: [CAViews].[uvw_fct_Barb_Spot_Augmented_CAISTHISNEEDED?] c
						inner join StationPrice.[StationPriceUpdate].[barb_conf] bc
							on  c.barb_area_no=bc.area_no
								and c.barb_station_no=bc.db2_station_no
								and c.sales_house_fkey=bc.sales_house_id
								and c.barb_log_station=bc.log_station_no
						cross join DateComparions dtc
					WHERE 1=1
						and sales_house_fkey in (17,26)
						and c.Spot_Date/100= dtc.YearMonth		--between 20161201 AND 20161231

						and CAST(c.spot_no AS BIGINT) NOT IN (	--tele sales spots need removing --- 20200213  v48 https://itvplc.jira.com/browse/ATSDW-2952
												Select
												spot_no
												From LandmarkII_ODS.AtsDwTransform.LmSpot spot
												Join
												LandmarkII_ODS.AtsDwTransform.Lmcamp_ddes tele
												On spot.camp_no=tele.camp_no
												Where tele.ddes_code='TEL' and brek_sched_date/100=dtc.YearMonth
											)
										
				) p
				UNPIVOT
				(
					[ViewingNumber] FOR [Demo] IN 
					(
					[qty_homes]
					,[qty_hwives]
					,[qty_hwschd]
					,[qty_hw1654]
					,[qty_hwabc1]
					,[qty_adults]
					,[qty_ad1624]
					,[qty_ad1634]
					,[qty_ad1654]
					,[qty_adabc1]
					,[qty_men]
					,[qty_me1634]
					,[qty_meabc1]
					,[qty_women]
					,[qty_wo1634]
					,[qty_woabc1]
					,[qty_childs]
					,[qty_chil49]
					)	
				) as unpv 
			)
			SELECT DISTINCT 
				--'BARBSPOT' AS 'BARBSPOTImpacts >>>>>>',
				sare_no as SalesAreaNo, 
				--'DemoNumber'=	isnull(( select top 1 demo_no from [LandmarkII_ODS].[AtsDwTransform].[LmDemo] where demo_abbreviation = demo ),-1), 
				'DemoNumber'=	isnull(( select top 1 demo_no from [LandmarkII_ODS].[AtsDwDbViews].[LmDemo_Mapped_StationPrice]  where demo_abbreviation = demo ),-1),  --HOUSPERSONS_refactor			 
				b.YearMonth as YearMonth,
				RatingsType = 'BS',
				b.BroadcastSource,
				[DurationImpacts]*100 as DurationImpacts, 
				[RatecardFactorImpacts]*100 as RatecardImpacts	
			--INTO [tempBARB_SPOT_Impacts]
			FROM BarbSpotData b
					JOIN StationPrice.[StationPriceUpdate].SysAppConfiguration AS sac
							ON b.sare_no=sac.SalesAreaNo AND b.BroadcastSource=sac.BroadcastSource
							AND sac.ImpactsCalculationFlag='Y' --and sac.LandmarkImpactsAvailable='Y'
								and b.YearMonth between FORMAT(CAST(sac.ValidFrom as DATE),'yyyyMM') and FORMAT(CAST(sac.ValidTo as DATE),'yyyyMM')		--LC --6
			WHERE ViewingNumber!=0
			ORDER BY 1,2;
    """

    # Execute the query and load the results into a pandas DataFrame
    df = pd.read_sql(query, engine)

    return df