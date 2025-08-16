library(sf)
library(terra)
library(dplyr)
library(fasterize)
library(exactextractr)


ecoregions <- st_read("C:/Users/azvol/Code/LandDegradation/sbtn-land-hub/thresholds-maps/Ecoregions2017/Ecoregions2017.shp") %>% 
    filter(REALM == "Neotropic")
ecoregions <- ecoregions[1:2,]
ecoregions

sdg151_raster_r <- terra::rast("C:/Data/Output/TrendsEarth_SDG15.3.1_2000-2023.tiff")

names(sdg151_raster_r) <- c(
    "sdg_2000_15",         # Band 1: SDG Indicator 15.3.1 for baseline (2000-2015)
    "prod_deg_2001_15",        # Band 2: Land Productivity Dynamics for 2001-2015
    "lc_deg_2000_15",      # Band 3: Land cover degradation for 2000-2015
    "soc_deg_2000_15",     # Band 4: Soil organic carbon degradation for 2000-2015
    "sdg_2004_19",         # Band 5: SDG Indicator 15.3.1 for 2004-2019
    "prod_deg_2004_19",        # Band 6: Land Productivity Dynamics for 2004-2019
    "lc_deg_2015_19",      # Band 7: Land cover degradation for 2015-2019
    "soc_deg_2015_19",     # Band 8: Soil organic carbon degradation for 2015-2019
    "sdg_status_19",         # Band 9: SDG Indicator 15.3.1 status in 2019
    "sdg_2008_23",         # Band 10: SDG Indicator 15.3.1 for 2008-2023
    "prod_deg_2008_23",        # Band 11: Land Productivity Dynamics for 2008-2023
    "lc_deg_2015_23",      # Band 12: Land cover degradation for 2015-2023
    "soc_deg_2015_23",     # Band 13: Soil organic carbon degradation for 2015-2023
    "sdg_status_23"          # Band 14: SDG Indicator 15.3.1 status in 2023
)

names(sdg151_raster_r)

sdg151_raster_r


# Function to get area-weighted fraction for each value in each band
get_fraction_table <- function(r, bands, zones) {
    # Create subset of raster with only the bands we want
    r_subset <- r[[bands]]
    
    # Single exact_extract call - optimized for performance and stability
    result <- exactextractr::exact_extract(r_subset, zones, function(df) {
        # Get ECO_ID for this zone
        eco_id <- if ("ECO_ID" %in% names(df)) df$ECO_ID[1] else NA
        
        # Pre-filter columns and handle edge cases - exclude coverage_fraction
        band_cols <- setdiff(names(df), c("area", "ECO_ID", "coverage_fraction"))
        if (length(band_cols) == 0 || nrow(df) == 0) {
            return(data.frame(ECO_ID = numeric(0), band = character(0), value = numeric(0), area = numeric(0)))
        }
        
        # Use vectorized operations instead of nested loops
        result_list <- vector("list", length(band_cols) * 50)  # Pre-allocate generously
        idx <- 1
        
        for (band_col in band_cols) {
            vals <- df[[band_col]]
            areas <- df$area
            
            # Remove NAs efficiently
            valid_idx <- !is.na(vals)
            if (sum(valid_idx) == 0) next
            
            vals <- vals[valid_idx]
            areas <- areas[valid_idx]
            
            # Use tapply for efficient grouping (much faster than loops)
            area_by_val <- tapply(areas, vals, sum, na.rm = TRUE)
            
            # Convert to vectors
            unique_vals <- as.numeric(names(area_by_val))
            unique_areas <- as.numeric(area_by_val)
            
            # Build result efficiently
            for (i in seq_along(unique_vals)) {
                if (idx <= length(result_list)) {
                    result_list[[idx]] <- data.frame(
                        ECO_ID = eco_id,
                        band = band_col,
                        value = unique_vals[i],
                        area = unique_areas[i],
                        stringsAsFactors = FALSE
                    )
                    idx <- idx + 1
                }
            }
        }
        
        # Clean up and combine
        result_list <- result_list[1:(idx-1)]  # Remove unused slots
        if (length(result_list) > 0) {
            do.call(rbind, result_list)
        } else {
            data.frame(ECO_ID = numeric(0), band = character(0), value = numeric(0), area = numeric(0))
        }
    }, progress=TRUE, include_area=TRUE, summarize_df=TRUE, include_cols="ECO_ID", 
    max_cells_in_memory=5e7)  # More conservative memory setting
    
    return(result)
}

# Baseline and 2023 status layer
fractions_by_band <- get_fraction_table(sdg151_raster_r, c(1, 14), ecoregions)


#plot(sdg151_raster, main = "Trends.Earth SDG15.3.1 (COG quicklook)")

plot(ecoregions, border='red', lwd=0.5)
