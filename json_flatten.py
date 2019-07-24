def flattenjson( b, delim ):
    val = {}
    for i in b.keys():
        if isinstance( b[i], dict ):
            get = flattenjson( b[i], delim )
            for j in get.keys():
                val[ i + delim + j ] = get[j]
        else:
            val[i] = b[i]

    return val

flattenjson( {
    "pk": 22, 
    "model": "auth.permission", 
    "fields": {
      "codename": "add_message", 
      "name": "Can add message", 
      "content_type": 8
    }
  }, "__" )
  
out_dict = flattenjson(input,"__")

with open('mycsvfile.csv', 'w') as f:  # Just use 'w' mode in 3.x
    w = csv.DictWriter(f, out_dict.keys())
    w.writeheader()
    w.writerow(out_dict)