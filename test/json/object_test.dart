library json.object_test;

import 'package:unittest/unittest.dart';

import '../../lib/json/object.dart';

class MockUser extends JsonObject {
  static final OBJECT_DATA =
    { 'user_id' : 454545,
      'screen_name' : 'fantastic_user',
      'aliases' : [ 'fantastic_man', 'super_fantastic_man' ],
      'posts' : [
         {
           'post_id': 14464,
           'title' : 'Fantastic events are fantastic',
           'mentioned_users' : [
             { 'user_id' : 22464,
               'screen_name' : 'fantastic_friend' 
             },
             { 'user_id' : 666,
               'screen_name' : 'fantastic_girl'
             }
           ],
           'content' : 'Wow, that event was fantastic'
         },
         {
           'post_id' : 22562,
           'title' : 'Even more fantastic events',
           'mentioned_users' : [],
           'content' : 'Not as fantastic as other events' 
         }
      ],
      'image' : 
        { 'alt' : 'fantastic_user_image',
          'href' : 'https://fantastic.com/image.jpg'
        }
    };
  
  int get userId => getField("user_id");
  
  String get screenName => getField("screen_name");
  set screenName(String value) => setField("screen_name", value);
  
  List<String> get aliases => getField("aliases");
  
  MockImage get image => new MockImage._delegate(this, "image");
  set image(MockImage image) => setField("image", image);
  
  String get nonExistentField => getField("field_a");
  set nonExistentField(String value) => setField("field_a", value);
  
  List<JsonObject> get posts => new JsonList(
      this, "posts", 
      (path) => new MockPost._delegate(this, path));
  
  MockUser.delegate(JsonObject obj, path, {String selector: "*"}):
    super.delegate(obj, path);
  
  MockUser(String selector) : 
    super(OBJECT_DATA, selector: selector);
  
}

class MockImage extends JsonObject {
  String get alt => getField("alt");
  set alt(String value) => setField("alt", value);
  String get href => getField("href");
  
  MockImage._delegate(JsonObject delegate, String pathToImage) :
    super.delegate(delegate, pathToImage);
  
  MockImage(String alt, String href) : super({'alt': alt, 'href':href});
}

class MockPost extends JsonObject {
  int get postId => getField("post_id");
  
  String get title => getField("title");
  set title(String value) => setField("title", value);
  
  List<JsonObject> get mentionedUsers => 
      new JsonList(this, "mentioned_users", 
          (path) => new MockUser.delegate(this, path, selector: "user_id,screen_name"));
  
  MockPost._delegate(JsonObject delegate, var pathToPost) :
    super.delegate(delegate, pathToPost);
  
}

void main() {
  group("json object", () {
    group("(all selected)", () {
      var mockObject = new MockUser("*");

      test("should be able to get the 'id'", () {
        expect(mockObject.userId, 454545);
      });
      test("should be able to get and set the 'screen name", () {
        expect(mockObject.screenName, "fantastic_user", reason: "before setting field");
        mockObject.screenName = "more_fantastic_user";
        expect(mockObject.screenName, "more_fantastic_user", reason: "after setting field");
      });
      test("should be able to get and set the image", () {
        var img = mockObject.image;
        expect(img.alt, "fantastic_user_image", reason: "before set alt");
        img.alt = "more_fantastic_image";
        expect(img.alt, "more_fantastic_image");
        var newImage = new MockImage("super_fantastic_image", "http://super_fantastic.com");
        mockObject.image = newImage;
        expect(mockObject.image.alt, "super_fantastic_image", reason: "after setting image");
        expect(mockObject.image.href, "http://super_fantastic.com", reason: "after setting image");
      });

      test("should be able to mutate aliases", () {
        expect(mockObject.aliases, ['fantastic_man', 'super_fantastic_man']);
        mockObject.aliases.add("mr_awesomely_fantastic");
        expect(mockObject.aliases, ['fantastic_man', 'super_fantastic_man', 'mr_awesomely_fantastic']);
      });
      
      test("should be able to set the value of a nonexistent field", () {
        expect(mockObject.nonExistentField, null, reason: "before set");
        mockObject.nonExistentField = "hello";
        expect(mockObject.nonExistentField, "hello", reason: "after set");
      });

      test("should be able to get the posts of the object", () {
        expect(mockObject.posts.map(
            (post) => post.getField("post_id")), [14464, 22562]);
      });
      
      test("modifying a post should modify the object", () {
        var post = mockObject.posts.first;
        print(post.toJson());
        expect(post.title, 'Fantastic events are fantastic', reason: 'before set');
        post.title = 'Even more fantastic events';
        expect(mockObject.posts.first.title, 'Even more fantastic events', reason: 'after set');
      });
      
      test("should be able to get the mentioned users from a post", () {
        var post = mockObject.posts.first;
        var mentioned = post.mentionedUsers.first;
        print(mentioned.toJson());
        expect(post.mentionedUsers.map(
            (user) => user.userId), [22464, 666]);
      });
      
    });
  });
  
}