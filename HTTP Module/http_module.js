function handleEtlImportRow(inputMap, context) {
  Log.info("Processing CSV row from file: " + context.filename);

  var filenames = ['file1.csv', 'file2.csv', 'file3.csv'];
  var url = 'http://server:port/path/';

  for(let i = 0; i < filenames.length; i++){
    var get = Http.get(url+filenames[i]);
    get.execute();
    
    if (!get.isSuccess()) {
      // Log.error(get.getFailureMessage());
      // return;
      throw get.getFailureMessage();
    }
    
    Log.info(get.getResponseString());

  }
}