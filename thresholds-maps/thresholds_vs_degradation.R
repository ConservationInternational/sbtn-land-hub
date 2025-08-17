library(sf)
library(terra)
library(dplyr)
library(fasterize)
library(exactextractr)
library(data.table)
library(tidyr)
library(readr)
library(stringr)


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
    
    # Vectorized SOC recoding - more efficient than loop
    soc_layers <- grepl("^soc_deg", names(r_subset))
    if (any(soc_layers)) {
        # Apply recoding only to SOC layers using vectorized operations
        r_subset[[which(soc_layers)]] <- ifel(
            r_subset[[which(soc_layers)]] < -10 & r_subset[[which(soc_layers)]] > -32768, -1,
            ifel(r_subset[[which(soc_layers)]] >= -10 & r_subset[[which(soc_layers)]] <= 10, 0,
            ifel(r_subset[[which(soc_layers)]] > 10, 1, r_subset[[which(soc_layers)]])))
    }
    
    # Process all bands at once using coverage_area
    coverage <- exactextractr::exact_extract(
        r_subset, 
        zones, 
        coverage_area = TRUE,
        include_cols = "ECO_ID",
        progress = TRUE,
        max_cells_in_memory = 1e8
    )
    
    # Handle the result - coverage_area returns a list with one data frame per feature
    if (is.list(coverage) && length(coverage) > 0) {
        
        # For multi-band input, columns should be named after each band
        band_names <- names(r_subset)
        value_cols <- intersect(band_names, names(coverage[[1]]))
        
        # Convert all zone data to data.table for fast processing
        zone_list <- vector("list", length(coverage))
        
        for (i in seq_along(coverage)) {
            zone_df <- coverage[[i]]
            zone_id <- if ("ECO_ID" %in% names(zone_df)) zone_df$ECO_ID[1] else i
            
            # Convert to data.table
            dt <- as.data.table(zone_df)
            dt[, zone_id := zone_id]
            
            # Keep only rows with valid coverage area
            dt <- dt[!is.na(coverage_area) & coverage_area > 0]
            
            if (nrow(dt) > 0) {
                zone_list[[i]] <- dt
            }
        }
        
        # Combine all zones into one large data.table
        all_zones_dt <- rbindlist(zone_list, fill = TRUE)
        
        if (nrow(all_zones_dt) > 0) {
            # Melt from wide to long format (much faster than manual loops)
            long_dt <- melt(all_zones_dt, 
                           id.vars = c("zone_id", "coverage_area"),
                           measure.vars = value_cols,
                           variable.name = "band",
                           value.name = "value")
            
            # Remove rows with NA values
            long_dt <- long_dt[!is.na(value)]
            
            # Aggregate coverage area by zone_id, band, and value (very fast with data.table)
            result_dt <- long_dt[, .(area = sum(coverage_area, na.rm = TRUE)), 
                                by = .(ECO_ID = zone_id, band, value)]
            
            # Filter out zero areas and convert back to data.frame
            result_dt <- result_dt[area > 0]
            result <- as.data.frame(result_dt)
            result$band <- as.character(result$band)
            
            return(result)
        }
    }
    
    # Return empty result if no data
    return(data.frame(ECO_ID = numeric(0), band = character(0), value = numeric(0), area = numeric(0)))
}

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
    right_join(fractions_by_band, by = "ECO_ID")
    
    %>%
    ggplot(aes(x = value, fill = metric)) +
    geom_histogram(position = "dodge", bins = 30) +
    facet_wrap(~ ECO_ID) +
    theme_minimal() +
    labs(title = "Combined Data vs. Fractions by Band",
         x = "Value",
         y = "Count")