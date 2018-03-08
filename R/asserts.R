assertValidPath<-function(path)
{
  checkmate::assertString(path, pattern= "[^\\0]+")
}
