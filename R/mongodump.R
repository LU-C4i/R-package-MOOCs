#' Import clickstream data into a mongodb
#'
#' This function imports session-based JSON clickstream files into a mongodb database.
#'
#' This function requires a running mongodb database (see: https://www.mongodb.org/)
#' @param path_to_clickstream Path to compressed archive with clickstream files
#' @param user Username for mongodb database. Defaults to NULL
#' @param password Password for mongodb database. Defaults to NULL
#' @param verbose Print verbose intermediate messages? Defaults to FALSE
#' @seealso \code{\link{sqldump}}
#' @examples \dontrun{
#' # Dump forum SQL file for federalism course
#' dumpSQL("/home/jasper/Documents/MOOC-DATA/Data_Dumps/federalism-001","forum",verbose = TRUE)
#' }
#' @author Jasper Ginn
#' @importFrom rmongodb mongo.create mongo.is.connected mongo.get.database.collections mongo.destroy
#' @importFrom DBI dbClearResult dbConnect dbSendQuery fetch dbDisconnect
#' @importFrom stringr str_extract str_replace_all
#' @importFrom R.utils gunzip
#' @export

mongodump <- function(path_to_clickstream,
                      database = 'moocs',
                      user = NULL,
                      password = NULL,
                      verbose = FALSE) {

  # verbose message
  verboseM <- function(path_to_clickstream, database, user, password) {
    q1 <- paste0("Now storing file ", "'",path_to_data,"'")
    q2 <- paste0("To database ", "'",sql_db,"'")
    q3 <- paste0("With username ", "'",user,"'", " and password ", "'",password,"'")
    cat(q1,"\n", q2, "\n",q3)
  }

  # Helper 1: Get list of collections in mongo db -----

  getTables <- function(user, password, database, collection) {
    # Create mongo connection to localhost
    mongo <- mongo.create(db = database)
    # Get all collections
    if(mongo.is.connected(mongo) == TRUE) {
      colls <- mongo.get.database.collections(mongo, database)
    }
    # Destroy
    mongo.destroy(mongo)
    # Check
    check <- ifelse(paste0(database, ".", collection) %in% colls, TRUE, FALSE)
    return(check)
  }

  # Helper 2: list files in folder, take the right one and return file, destination file etc. in a list ----

  folderIndex <- function(path_to_clickstream, user, password, database) {
    # Take course name
    course_name <- str_split(path_to_clickstream, "/")
    course_name <- gsub("_clickstream_export.gz", "", course_name[[1]][length(course_name[[1]])])
    # strip special characters
    course_name <- str_replace_all(course_name, "[[:punct:]]", "")
    # Check if exists
    check <- getTables(user, password, database, course_name)
    if(check == TRUE) {
      stop("Collection already exists in mongo database.")
    }
    # Create filepath for unzipped file
    UF <- gsub(".gz", "", path_to_clickstream)
    # unzip if not exists
    if(!file.exists(UF)) {
      gunzip(path_to_clickstream, destname = UF, remove = F)
    } else {
      print("File already unzipped. Moving on.")
    }
    # Return list with info
    return(list("collection" = course_name,
                "unzipped_file_path" = UF))
  }

  # Helper 2: Send queries to SQL -----

  SQLQuery <- function(query, user, password) {
    # Connect to mysql db
    db <- dbConnect(MySQL(), username = user, password=password)
    # Send query
    res <- dbSendQuery(db, query)
    # Clear result
    dbClearResult(res)
    # Disconnect
    dbDisconnect(db)
    return(TRUE)
  }

  # Call helper 1
  fp_index <- folderIndex(path_to_data, type, user, password)

  # If verbose
  if(verbose == TRUE) {
    verboseM(path_to_data, fp_index$course_name_type, user, password)
  }

  # Drop table
  res <- SQLQuery(paste0("DROP DATABASE IF EXISTS ", fp_index$course_name_type, ";"), user, password)
  # Create table
  res <- SQLQuery(paste0("CREATE DATABASE ", fp_index$course_name_type, ";"), user, password)

  # Dump sql file
  command_to_system <- paste0("mysql -u ", user, " -p", password, " -D ", fp_index$course_name_type, " < ", fp_index$sql_file)
  system(command_to_system)

  # Remove unzipped files
  res <- unlink(fp_index$sql_file_folder, recursive = T)

}
