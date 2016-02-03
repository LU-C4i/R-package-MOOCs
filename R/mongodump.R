#' Import clickstream data into a mongodb
#'
#' This function imports session-based JSON clickstream files into a mongodb database.
#'
#' This function requires a running mongodb database (see: https://www.mongodb.org/)
#' @param path_to_clickstream Path to compressed archive with clickstream file
#' @param database database in which to store the collection of clickstream data (collection will be defined as course name)
#' @param create_index Mongo can create an index on usernames, which speeds up queries considerably. Defaults to TRUE
#' @param index_value Which value will mongodb use for indexing?
#' @param verbose Print verbose intermediate messages? Defaults to FALSE
#' @param jsonArray Use the --jsonArray flag while importing documents to mongo. This treats the input as a jsonArray (see https://docs.mongodb.org/manual/reference/program/mongoimport/)
#' @param ... any other arguments needed to connect to mongodb (e.g. username, password, host)
#' @seealso \code{\link{sqldump}}
#' @examples \dontrun{
#' # Dump clickstream file into mongo
#' mongodump("/home/jasper/Documents/MOOC-DATA/Data_Dumps/configuringworld-001/configuringworld-001_clickstream_export.gz",
#' verbose = TRUE)
#' }
#' @author Jasper Ginn
#' @importFrom rmongodb mongo.create mongo.is.connected mongo.get.database.collections mongo.destroy mongo.index.create
#' @importFrom stringr str_extract str_replace_all str_split
#' @importFrom R.utils gunzip
#' @export

mongodump <- function(path_to_clickstream,
                      database = 'moocs',
                      create_index = TRUE,
                      index_value = "username",
                      verbose = FALSE,
                      jsonArray = FALSE,
                      ...) {

  # verbose message
  verboseM <- function(path_to_clickstream, database, collection, ...) {
    q1 <- paste0("Now storing file ", "'",path_to_clickstream,"'")
    q2 <- paste0("To database ", "'",database,"'", " and collection ", "'", collection, "'")
    q3 <- ifelse(create_index == T,
                 paste0("Mongo will create an index on ", index_value, " to increase lookup speed"),
                 paste0("Mongo will not create an index on  ", index_value))
    cat(q1,"\n", q2, "\n",q3)
  }

  # Helper 1: Get list of collections in mongo db -----

  getTables <- function(database, collection, ...) {
    # Create mongo connection to localhost
    mongo <- mongo.create(db = database, ...)
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

  folderIndex <- function(path_to_clickstream, database, ...) {
    # Take course name
    course_name <- str_split(path_to_clickstream, "/")
    course_name <- gsub("_clickstream_export.gz", "", course_name[[1]][length(course_name[[1]])])
    # If file extension, remove
    course_name <- "configuringworld-001.txt"
    check <- ifelse(length(unlist(str_split(course_name, "\\."))) > 1,
                    TRUE,
                    FALSE)
    if(check == TRUE) {
      course_name <- str_split(course_name, "\\.")[[1]][1]
    }
    # strip special characters
    course_name <- str_replace_all(course_name, "[[:punct:]]", "")
    # Check if exists
    check <- getTables(database, course_name, ...)
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

  # Call helper 1
  fp_index <- folderIndex(path_to_clickstream, database, ...)

  # If verbose
  if(verbose == TRUE) {
    verboseM(path_to_clickstream, database, fp_index$collection, ...)
  }

  # Dump table
  command_to_system <- ifelse(jsonArray == FALSE,
                              paste0("mongoimport -d ", database,
                              " -c ", fp_index$collection, " --type json --file ",
                              fp_index$unzipped_file_path), # If no JSON array
                              paste0("mongoimport -d ", database,
                                     " -c ", fp_index$collection, " --type json --file ",
                                     fp_index$unzipped_file_path, " --jsonArray")) # If JSON array
  system(command_to_system)

  # Create index on usernames to increase speed
  if(create_index == TRUE) {
    if(getTables(database, fp_index$collection) != TRUE) {
      stop(paste0("Collection was not imported properly. Cannot create index on ", index_value, "."))
    } else {
      # Create mongo connection to localhost
      mongo <- mongo.create(db = database, ...)
      # Create index
      if(mongo.is.connected(mongo) == TRUE) {
        mongo.index.create(mongo, paste0(database, ".", fp_index$collection), list(index_value=1))
      }
      # Destroy
      mongo.destroy(mongo)
    }
  }

  # Remove unzipped files
  res <- file.remove(fp_index$unzipped_file_path)

  # Return TRUE
  return(TRUE)
}
