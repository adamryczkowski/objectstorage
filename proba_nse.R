proba<-function(..., .flag1=FALSE, .opt1='bla') {
  obj_expr<-rlang::enquos(..., .homonyms = 'error')
  storage_path<-rlang::get_expr(obj_expr[[1]])
  obj_expr<-obj_expr[-1]
  no_names<-which(names(obj_expr)=="")
  #browser()
  if(length(no_names)>0) {
    expr_str<-list()
    for(expr in obj_expr[no_names]) {
      expr<-rlang::get_expr(expr)
      expr_str[length(expr_str)+1]<-deparse(expr)
    }
    stop(paste0("Expression", ifelse(length(no_names)>1, "s",""), " ", paste0("\"", expr_str, "\"", collapse=", "),
                ifelse(length(no_names)>1, " are", " is"), " not named. ",
                "All objects put into the objectstorage must be named. ",
                "To specify a name manually use named list as an input. ",
                "Use syntax save_objects(obj_name=<value>) or save_objects(obj_name:=<value>)."))
  }
  dups<-duplicated(names(obj_expr))
  if(any(dups)) {
    stop(paste0("Symbol", ifelse(sum(dups)>1,'s',''), " ", paste0('"', names(obj_expr)[dups], '"', collapse=', '), ifelse(sum(dups)>1,' are',' is'),
                " duplicated. Each object is uniqually determined by its name and it makes no sense in storing mutliple objects with the same name."))
  }
  browser()
  ans<-rlang::list2(obj_expr)
  return(purrr::map(ans[[1]], rlang::eval_tidy))
}

a=10

x<-proba(".path", .flag1=TRUE, b=1:20, c:=1, d=a, .opt1=10, a=1, bc=1:10)
