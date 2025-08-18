library(sf)
library(terra)
library(dplyr)
library(fasterize)
library(exactextractr)
library(data.table)
library(tidyr)
library(readr)
library(stringr)
library(purrr)


ecoregions <- st_read("C:/Users/azvol/Code/LandDegradation/sbtn-land-hub/thresholds-maps/Ecoregions2017/Ecoregions2017.shp") %>% 
    filter(REALM == "Neotropic")
ecoregions <- ecoregions[1:2,]
#ecoregions

sdg151_raster_r <- terra::rast("C:/Data/Output/TrendsEarth_SDG15.3.1_2000-2023.tiff")

names(sdg151_raster_r) <- c(
    "sdg_2000_15",       # Band 1: SDG Indicator 15.3.1 for baseline (2000-2015)
    "prod_deg_2001_15",  # Band 2: Land Productivity Dynamics for 2001-2015
    "lc_deg_2000_15",    # Band 3: Land cover degradation for 2000-2015
    "soc_deg_2000_15",   # Band 4: Soil organic carbon degradation for 2000-2015
    "sdg_2004_19",       # Band 5: SDG Indicator 15.3.1 for 2004-2019
    "prod_deg_2004_19",  # Band 6: Land Productivity Dynamics for 2004-2019
    "lc_deg_2015_19",    # Band 7: Land cover degradation for 2015-2019
    "soc_deg_2015_19",   # Band 8: Soil organic carbon degradation for 2015-2019
    "sdg_status_19",     # Band 9: SDG Indicator 15.3.1 status in 2019
    "sdg_2008_23",       # Band 10: SDG Indicator 15.3.1 for 2008-2023
    "prod_deg_2008_23",  # Band 11: Land Productivity Dynamics for 2008-2023
    "lc_deg_2015_23",    # Band 12: Land cover degradation for 2015-2023
    "soc_deg_2015_23",   # Band 13: Soil organic carbon degradation for 2015-2023
    "sdg_status_23"      # Band 14: SDG Indicator 15.3.1 status in 2023
)

names(sdg151_raster_r)

sdg151_raster_r


# Function to get area-weighted fraction for each value in each band
get_fraction_table <- function(r, bands, zones) {
    # Create subset of raster with only the bands we want
    r_subset <- r[[bands]]
    
    # Use exact_extract to get coverage_area data for each zone
    # This efficiently extracts pixel values and their coverage areas
    coverage_list <- exactextractr::exact_extract(
        r_subset, 
        zones, 
        coverage_area = TRUE,
        include_cols = "ECO_ID",
        progress = TRUE,
        max_cells_in_memory = 1e8
    )
    
    # Process the results outside of exact_extract
    if (length(coverage_list) > 0) {
        # Convert list of data frames to long format with aggregation
        result_list <- vector("list", length(coverage_list))
        
        for (i in seq_along(coverage_list)) {
            df <- coverage_list[[i]]
            eco_id <- df$ECO_ID[1]  # Get the ECO_ID for this zone
            
            # Get band columns (exclude coverage_area and ECO_ID)
            band_cols <- setdiff(names(df), c("coverage_area", "ECO_ID"))
            
            # Convert to long format and aggregate
            long_df <- df %>%
                tidyr::pivot_longer(
                    cols = dplyr::all_of(band_cols),
                    names_to = "band", 
                    values_to = "value"
                ) %>%
                dplyr::filter(!is.na(.data$value) & .data$coverage_area > 0) %>%
                dplyr::group_by(.data$band, .data$value) %>%
                dplyr::summarise(area = sum(.data$coverage_area, na.rm = TRUE), .groups = "drop") %>%
                dplyr::filter(.data$area > 0) %>%
                dplyr::mutate(ECO_ID = eco_id)
            
            result_list[[i]] <- long_df
        }
        
        # Combine all results
        combined_result <- dplyr::bind_rows(result_list) %>%
            dplyr::select("ECO_ID", "band", "value", "area") %>%
            dplyr::arrange(.data$ECO_ID, .data$band, .data$value)
        
        return(as.data.frame(combined_result))
    }
    
    # Return empty result if no data
    return(data.frame(ECO_ID = numeric(0), band = character(0), value = numeric(0), area = numeric(0)))
}


fractions_by_band <- get_fraction_table(sdg151_raster_r, c(1,2), ecoregions)

fractions_by_band <- get_fraction_table(sdg151_raster_r, c(1,2,3,6,7,14), ecoregions)

fractions_by_band <- get_fraction_table(sdg151_raster_r, c(4), ecoregions)
#fractions_by_band <- get_fraction_table(sdg151_raster_r, c(1:14), ecoregions)

#########################
# Load CSVs

csv_path <- "C:/Users/azvol/Conservation International Foundation/Jordan Rogan - To share with Alex/"

# Load and add indicator columns in one step
ndep_data <- read_csv(file.path(csv_path, "Neotropic Ndep.csv"), col_types = cols(.default = "c")) %>% 
    mutate(indicator = "nitrogen_deposition")

soc_data <- read_csv(file.path(csv_path, "Neotropic SOC.csv"), col_types = cols(.default = "c")) %>% 
    mutate(indicator = "soil_organic_carbon")

soil_erosion_data <- read_csv(file.path(csv_path, "Neotropic soil erosion.csv"), col_types = cols(.default = "c")) %>% 
    mutate(indicator = "soil_erosion")

nat_land_data <- read_csv(file.path(csv_path, "Neotropic nat land.csv"), col_types = cols(.default = "c")) %>% 
    mutate(indicator = "natural_land")

# Combine all datasets and convert to long format
combined_data <- bind_rows(ndep_data, soc_data, soil_erosion_data, nat_land_data) %>%
    # Clean column names
    rename_with(~ make.names(.) %>% str_replace_all("\\.", "_")) %>%
    # Convert to long format
    pivot_longer(
        cols = -c(ECO_ID, contains("Ecoregion"), indicator),
        names_to = "metric",
        values_to = "value"
    ) %>%
    # Convert ECO_ID and value to appropriate types
    mutate(
        ECO_ID = as.numeric(ECO_ID),
        value = as.numeric(value)
    ) %>%
    # Clean up and filter
    filter(!is.na(ECO_ID) & !is.na(value)) %>%
    mutate(metric = str_replace_all(metric, "_+", "_") %>% str_remove("_$")) %>%
    select(-Ecoregion_name)

# Display results
cat("Combined data has", nrow(combined_data), "rows\n")
print(head(combined_data, 10))

###########################
# Join data and plot results

# Reshape combined_data to wide format with metric values in columns

combined_data_wide <- combined_data %>%
    pivot_wider(names_from = metric, values_from = value)

# Now join with the SDG 15.3.1 data from the rasters
combined_data_wide %>%
    right_join(fractions_by_band, by = "ECO_ID") %>%
    ggplot(aes(x = value, fill = metric)) +
    geom_histogram(position = "dodge", bins = 30) +
    facet_wrap(~ ECO_ID) +
    theme_minimal() +
    labs(title = "Combined Data vs. Fractions by Band",
         x = "Value",
         y = "Count")