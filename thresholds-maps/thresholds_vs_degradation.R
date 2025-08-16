library(sf)
library(terra)
library(dplyr)
library(fasterize)
library(exactextractr)


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
        
        # Convert from wide format (one column per band) to long format
        result_list <- vector("list", length(coverage))
        
        for (i in seq_along(coverage)) {
            zone_df <- coverage[[i]]
            zone_id <- if ("ECO_ID" %in% names(zone_df)) zone_df$ECO_ID[1] else i
            
            # Reshape from wide to long format
            long_list <- vector("list", length(value_cols))
            
            for (j in seq_along(value_cols)) {
                band_name <- value_cols[j]
                band_values <- zone_df[[band_name]]
                areas <- zone_df$coverage_area
                
                # Remove NA values 
                valid_idx <- !is.na(band_values) & !is.na(areas)
                if (sum(valid_idx) > 0) {
                    valid_values <- band_values[valid_idx]
                    valid_areas <- areas[valid_idx]
                    
                    # Aggregate areas by value using tapply (sum areas for each unique value)
                    area_by_value <- tapply(valid_areas, valid_values, sum, na.rm = TRUE)
                    
                    # Convert to data frame and filter out zero areas
                    unique_values <- as.numeric(names(area_by_value))
                    total_areas <- as.numeric(area_by_value)
                    
                    # Keep only non-zero areas
                    nonzero_idx <- total_areas > 0
                    if (sum(nonzero_idx) > 0) {
                        long_list[[j]] <- data.frame(
                            ECO_ID = zone_id,
                            band = band_name,
                            value = unique_values[nonzero_idx],
                            area = total_areas[nonzero_idx],
                            stringsAsFactors = FALSE
                        )
                    }
                }
            }
            
            # Combine all bands for this zone
            valid_bands <- long_list[!sapply(long_list, is.null)]
            if (length(valid_bands) > 0) {
                result_list[[i]] <- do.call(rbind, valid_bands)
            }
        }
        
        # Combine all zones
        valid_zones <- result_list[!sapply(result_list, is.null)]
        if (length(valid_zones) > 0) {
            result <- do.call(rbind, valid_zones)
            rownames(result) <- NULL
            return(result)
        }
    }
    
    # Return empty result if no data
    return(data.frame(ECO_ID = numeric(0), band = character(0), value = numeric(0), area = numeric(0)))
}

# Process all bands
fractions_by_band <- get_fraction_table(sdg151_raster_r, c(1,2,3,6,7,14), ecoregions)
#fractions_by_band <- get_fraction_table(sdg151_raster_r, c(1:14), ecoregions)

#plot(ecoregions, border='red', lwd=0.5)
