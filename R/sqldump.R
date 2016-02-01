#' Import session-based SQL files in a MySQL database
#'
#' This function imports session-based SQL files into a MySQL database.
#'
#' This function requires a running MySQL database (see: https://www.mysql.com/)
#' @param path_to_data Folder with data files in it
#' @param type Which dataset should be stored?
#' @param user Username for MySQL database. Defaults to 'root'
#' @param password Password for MySQL database. Defaults to 'root'
#' @param verbose Print verbose intermediate messages? Defaults to FALSE
#' @seealso \code{\link{mongodump}}
#' @examples \dontrun{
#' # Dump forum SQL file for federalism course
#' dumpSQL("/home/jasper/Documents/MOOC-DATA/Data_Dumps/federalism-001","forum",verbose = TRUE)
#' }
#' @author Jasper Ginn
#' @importFrom RMySQL MySQL
#' @importFrom DBI dbClearResult dbConnect dbSendQuery fetch dbDisconnect
#' @importFrom stringr str_extract str_replace_all
#' @importFrom R.utils gunzip
#' @export

dumpSQL <- function(path_to_data,
                    type = c("general", "forum", "unanonymizable", "hash_mapping"),
                    #append_to_dataMap = F, # Append the dataset to a dataset keeping track of all table names?
                    user = "root",
                    password = "root",
                    verbose = FALSE) {
  # match arg
  type = match.arg(type)

  # verbose message
  verboseM <- function(path_to_data, sql_db, user, password) {
    q1 <- paste0("Now storing file ", "'",path_to_data,"'")
    q2 <- paste0("To database ", "'",sql_db,"'")
    q3 <- paste0("With username ", "'",user,"'", " and password ", "'",password,"'")
    cat(q1,"\n", q2, "\n",q3)
  }

  # Helper 1: Get list of tables in MySQL db -----

  getTables <- function(user, password, database) {
    # Connect to mysql db
    db <- dbConnect(MySQL(), username = user, password=password)
    # Statement
    query <- "SHOW DATABASES;"
    # List databases
    res <- dbSendQuery(db, query)
    # Fetch
    fetchRes <- fetch(res, n=-1)
    # Clear result
    dbClearResult(res)
    # Disconnect
    dbDisconnect(db)
    # Check
    check <- ifelse(database %in% fetchRes[,1], TRUE, FALSE)
    return(check)
  }

  # Helper 2: list files in folder, take the right one and return file, destination file etc. in a list ----

  folderIndex <- function(path_to_data, type, user, password) {
    # List files in data folder
    files <- list.files(path_to_data)
    # Grep the one that matches type
    file <- files[grepl(type, tolower(files))]
    # Take course name
    course_name <- str_extract(file, "\\(([^\\)]+)\\)")
    # strip special characters
    course_name <- str_replace_all(course_name, "[[:punct:]]", "")
    # append type
    course_name <- paste0(course_name, "_", type)
    # Check if exists
    check <- getTables(user, password, course_name)
    if(check == TRUE) {
      stop("Table already exists in MySQL database.")
    }
    # create temporary folder for unzipped files
    tempFolder <- paste0(type, "_unzipped")
    # Create path to zipped file
    fPath <- paste0(path_to_data, "/", file)
    # filepath for unzipped sql file
    sql_fp <- paste0(path_to_data, "/", tempFolder, "/", course_name, ".sql")
    # unzip if not exists
    if(!paste0(course_name, ".sql") %in% list.files(sql_fp)) {
      gunzip(fPath, destname = sql_fp, remove = F)
    } else {
      print("File already unzipped. Moving on.")
    }
    # Return list with info
    return(list("gzip_file" = fPath,
                "sql_file_folder" = paste0(path_to_data, "/", tempFolder),
                "sql_file" = sql_fp,
                "course_name_type" = course_name))
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

  # ADD LATER: APPEND THE COURSE / SQL FILE TO A DATA MAP R FILE

  # Return TRUE
  return(TRUE)
}
