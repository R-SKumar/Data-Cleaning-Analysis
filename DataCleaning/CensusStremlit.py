
import pandas as pd
import mysql.connector
import streamlit as st

def runQuery(query):
    try:        
        conn = mysql.connector.connect(
            host="localhost",
            user="root",
            password="",
            database="CensusDB"
        )
        cursor = conn.cursor()
        cursor.execute(query)
        data = cursor.fetchall()
        cursor.close()
        conn.close()
        return data
    except Exception as e:
        return f"An error occurred: {e}"

# SQL queries
queries = {
    "Total Population of Each District": f"""
        SELECT `District`, SUM(`Population`) as TotalPopulation
        FROM census
        GROUP BY `District`
        ORDER BY 2 DESC;
    """,
    "Literate Males and Females in Each District": f"""
        SELECT `District`, SUM(`Literate_Male`) as LiterateMales, 
        SUM(`Literate_Female`) as LiterateFemales
        FROM census
        GROUP BY `District`
        ORDER BY 1;
    """,
    "Percentage of Workers in Each District": f"""
        SELECT `District`, 
        (SUM(`Male_Workers` + `Female_Workers`) / SUM(`Population`) * 100) as WorkerPercentage
        FROM census
        GROUP BY `District`
        ORDER BY 2 DESC;
    """,
    "Households with LPG/PNG as Cooking Fuel in Each District": f"""
        SELECT District, SUM(LPG_or_PNG_Households) as LPG_PNG
        FROM census
        GROUP BY District
        ORDER BY 2 DESC;
    """,
    "Religious Composition of Each District": f"""
        SELECT District, 
        SUM(Hindus) as Hindus, 
        SUM(Muslims) as Muslims, 
        SUM(Christians) as Christians, 
        SUM(Sikhs) as Sikhs, 
        SUM(Buddhists) as Buddhists, 
        SUM(Jains) as Jains, 
        SUM(Others_Religions) as Others,
        SUM(Religion_Not_Stated) as NotStated
        FROM census
        GROUP BY District
        ORDER BY 1;
    """,
    "Households with Internet Access in Each District": f"""
        SELECT District, SUM(Households_with_Internet) as InternetAccess
        FROM census
        GROUP BY District
        ORDER BY 2 DESC;
    """,
    "Educational Attainment Distribution in Each District": f"""
        SELECT District, 
        SUM(Below_Primary_Education) as BelowPrimary, 
        SUM(Primary_Education) as `Primary`, 
        SUM(Middle_Education) as Middle, 
        SUM(Secondary_Education) as Secondary,
        SUM(Higher_Education) as Higher,
        SUM(Graduate_Education) as Graduate,
        SUM(Other_Education) as Other
        FROM census
        GROUP BY District
        ORDER BY 1;
    """,
    "Households with Access to Various Modes of Transportation in Each District": f"""
        SELECT District, 
        SUM(Households_with_Bicycle) as Bicycle, 
        SUM(Households_with_Car_Jeep_Van) as Car,         
        SUM(Households_with_Scooter_Motorcycle_Moped) as Scooter
        FROM census
        GROUP BY District
        ORDER BY 1;
    """,
    "Condition of Occupied Census Houses in Each District": f"""
        SELECT District, 
        SUM(Condition_of_occupied_census_houses_Dilapidated_Households) as Dilapidated, 
        SUM(Households_with_separate_kitchen_Cooking_inside_house) as SeparateKitchen, 
        SUM(Having_bathing_facility_Total_Households) as BathingFacility, 
        SUM(Having_latrine_facility_within_the_premises_Total_Households) as LatrineFacility,
        SUM(Ownership_Owned_Households) as Owner,
        SUM(Ownership_Rented_Households) as Rented
        FROM census
        GROUP BY District
        ORDER BY 1;
    """,
    "Household Size Distribution in Each District": f"""
        SELECT District, 
        SUM(Household_size_1_person_Households) as `1Person`, 
        SUM(Household_size_2_persons_Households) as `2Persons`, 
        SUM(Household_size_1_to_2_persons) as `1-2Persons`,
        SUM(Household_size_3_persons_Households) as `3Persons`,
        SUM(Household_size_3_to_5_persons_Households) as `3-5Persons`,
        SUM(Household_size_4_persons_Households) as `4Persons`,
        SUM(Household_size_5_persons_Households) as `5Persons`,
        SUM(Household_size_6_8_persons_Households) as `6-8Persons`,
        SUM(Household_size_9_persons_and_above_Households) as `9abovPersons`
        FROM census
        GROUP BY District
        ORDER BY 1;
    """,
    "Total Number of Households in Each State": f"""
        SELECT `State/UT` as State, 
        SUM(Households) as TotalHouseholds
        FROM census
        GROUP BY `State/UT`
        ORDER BY 2 DESC;
    """,
    "Households with Latrine Facility within Premises in Each State": f"""
        SELECT `State/UT` as State,
        SUM(Having_latrine_facility_within_the_premises_Total_Households) as Latrine_Facility_Within
        FROM census
        GROUP BY `State/UT`
        ORDER BY 2 DESC;
    """,
    "Average Household Size in Each State": f"""
        SELECT `State/UT` as State,
        ROUND(AVG(Households), 2) as Average_Household_Size
        FROM census
        GROUP BY `State/UT`
        ORDER BY 2 DESC;
    """,
    "Households Owned versus Rented in Each State": f"""
        SELECT `State/UT` as State,
        SUM(Ownership_Owned_Households) as Owned, 
        SUM(Ownership_Rented_Households) as Rented
        FROM census
        GROUP BY `State/UT`
        ORDER BY 1;
    """,
    "Distribution of Different Types of Latrine Facilities in Each State": f"""
        SELECT `State/UT` as State,
        SUM(Type_of_latrine_facility_Pit_latrine_Households) as Pit, 
        SUM(Type_of_latrine_facility_Night_soil_disposed_into_open_drain_Hou) as NightSoil, 
        SUM(Type_of_latrine_facility_Flush_pour_flush_latrine_connected_to_o) as Flush, 
        SUM(Type_of_latrine_facility_Other_latrine_Households) as Others
        FROM census
        GROUP BY `State/UT`
        ORDER BY 1;
    """,
    "Households with Access to Drinking Water Sources Near Premises in Each State": f"""
        SELECT `State/UT` as State,
        SUM(Main_source_of_drinking_water_Un_covered_well_Households) as UnCoveredWell,
        SUM(Main_source_of_drinking_water_Handpump_Tubewell_Borewell_Househo) as Handpump,
        SUM(Main_source_of_drinking_water_Spring_Households) as Spring,
        SUM(Main_source_of_drinking_water_River_Canal_Households) as River_Canal,
        SUM(Main_source_of_drinking_water_Other_sources_Spring_River_Canal_T) as RiverOthers,
        SUM(Main_source_of_drinking_water_Other_sources_Households) as Others
        FROM census
        GROUP BY `State/UT`
        ORDER BY 1;
    """,
    "Average Household Income Distribution in Each State": f"""
        SELECT `State/UT` as State,
        AVG(Power_Parity_Less_than_Rs_45000) as `Less45K`,         
        AVG(Power_Parity_Rs_45000_90000) as `45K-90K`,         
        AVG(Power_Parity_Rs_90000_150000) as `90K-150K`,         
        AVG(Power_Parity_Rs_45000_150000) as `45K-150K`,         
        AVG(Power_Parity_Rs_150000_240000) as `150K-240K`,         
        AVG(Power_Parity_Rs_240000_330000) as `240K-330K`,         
        AVG(Power_Parity_Rs_150000_330000) as `150K-330K`,         
        AVG(Power_Parity_Rs_330000_425000) as `330K-425K`,         
        AVG(Power_Parity_Rs_425000_545000) as `425K-545K`,         
        AVG(Power_Parity_Rs_330000_545000) as `330K-545K`,         
        AVG(Power_Parity_Above_Rs_545000) as `Above545K`,         
        AVG(Total_Power_Parity) as `Total`        
        FROM census
        GROUP BY `State/UT`
        ORDER BY 13 DESC;
    """,
    "Percentage of Married Couples with Different Household Sizes in Each State": f"""
        SELECT `State/UT` as State,
        SUM(Household_size_1_person_Households)/SUM(Married_couples_1_Households) * 100 as Married_Couples_1_Person        
        FROM census
        GROUP BY `State/UT`
        ORDER BY State;
    """,
    "Households Below the Poverty Line in Each State": f"""
        SELECT `State/UT` as State,
        SUM(Power_Parity_Less_than_Rs_45000) as Below_Poverty
        FROM census
        GROUP BY `State/UT`
        ORDER BY 2 DESC;
    """,
    "Overall Literacy Rate in Each State": f"""
        SELECT `State/UT` as State,
        SUM(Literate) / SUM(Population) * 100 as Literacy_Rate
        FROM census
        GROUP BY `State/UT`
        ORDER BY 2 DESC;
    """
}


# Streamlit application
st.title("Census Data Analysis")
st.write("This application displays various census data based on queries.")

# Sidebar for navigation
queryName = st.sidebar.selectbox("Select Query", list(queries.keys()))

# Run selected query and display results
if queryName:
    query = queries[queryName]
    print(query)
    # result, columns = runQuery(query)
    # df = pd.DataFrame(result, columns=columns)
    # df = pd.DataFrame(result, columns=[col.split()[-1] for col in queries[queryName].split("SELECT")[1].split("FROM")[0].split(",")])
    result = runQuery(query)
    colm = [col.split()[-1] for col in query.split("SELECT")[1].split("FROM")[0].replace("`","").split(",")]
    df = pd.DataFrame(result, columns=colm)
    
    print(colm)
    
    st.subheader(queryName)
    st.dataframe(df)

    # Display the data in charts if applicable
    if "Percentage" in queryName or "Rate" in queryName:
        st.bar_chart(df.set_index(df.columns[0]))
        st.line_chart(df.set_index(df.columns[0]))
    else:
        st.bar_chart(df.set_index(df.columns[0]))
        st.line_chart(df.set_index(df.columns[0]))