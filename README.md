# Google Cloud Storage #

A `dart` library for interacting with the [Google Cloud Storage][0] v1 APIs with full support for partial requests and `PATCH` updates.

** Note:** Due to a dependency conflict, this package is not currently usable from applications which import `'dart:html'`. This will be fixed in a future version.


## Usage ##

Include the following in your `pubspec.yaml`

    dependencies:
      google_cloud_storage: any
      
### Server applications ###

If using the library in a server application, import the `server` version of the library using,
    
    'import 'google_cloud_storage/cloud_storage_server.dart';
    
There are two ways to connect to the cloud storage API from server applications:
#### From Compute Engine

To connect to the API from a VM instance running on [Google Compute Engine][1], only the name of the project and the the project number need to be provided

eg. 
     
     CloudStorageConnection.open(<project_id>, <project_number>).then((connection) {
         //Do something with connection.
     });
     
#### Connecting via a service account

To connect to the API from a service account, provide the details of the service account as named arguments (and a [PermissionRole] which specifies the level of access to provide to the connection) to the constructor

eg.

    CloudStorageConnection.open(
        <project_id>, //The google assigned project identifier
        <project_number>, //The google assigned project number
        serviceAccount: <service_account_email>,
        pathToPrivateKey: <path>, //The (local) filesystem path containing a `.pem` file used to authenticate the service account
        role: <role> //The permission to grant to the service account. Defaults to `READER` access.
    ).then((connection) {
       //Do something with the connection.
    });
    
## Usage (Browser applications) ##

Include the following in your `pubspec.yaml`

    dependencies:
      google_cloud_storage: any
      
The library can then be imported using
    
    'import 'google_cloud_storage/cloud_storage_server.dart';
    
Create a new instance of the [CloudStorageConnection] class, with the details of a client that is authorised to connect to the cloud storage instance and the and the appropriate authorisation level for the client.

    CloudStorageConnection connection = new CloudStorageConnection(
        <project_id>, // The google assigned identifier of the project owner (the owner of the bucket/s to connect to)
        <auth> //An authentication context created with the `google_auth2client` library.
    );
    
    


[0]: https://cloud.google.com/products/cloud-storage/
[1]: https://cloud.google.com/products/compute-engine/