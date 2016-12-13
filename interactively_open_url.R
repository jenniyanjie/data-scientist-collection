
# this script is for interactively open webpage of apps from apps_meta_info
# assume the data.table 'tmp' is like this:
#  ---------------------------
# || id  |   preview_url    ||
#  ---------------------------
# || 1   |   www.google.com ||
# || 2   |  www.yahoo.com   ||
# || ... |  ....            ||
#  ---------------------------

library(data.table)

# the pause function is to wait for user action
pause <- function () {
  if (interactive()) {
    invisible(readline(prompt = "Press <Enter> to continue..."))
  } else {
    cat("Press <Enter> to continue...\n")
    invisible(readLines(file("stdin"), 1))
  }
}

ids <- tmp$id # or whatever filter for the page you want to open
for (i in ids) {
  url <- subset(tmp, id == i)$preview_url
  browseURL(url, browser = getOption("browser"))
  cat('now open: ', preview_url,'\n',sep = "")
  pause()
}


