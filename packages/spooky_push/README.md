# spooky_push

Native push for sp00ky Flutter apps: APNs on iOS, FCM on Android. The OS half
of `db.push` (spooky_core): permission, device token, what arrives, what the
user taps. What gets pushed is decided on the server by the `push:` rules in
`sp00ky.yml`; this plugin keeps the device registered for whoever is signed in.

No Firebase setup in the app: iOS talks to APNs directly, and Android gets its
Firebase client config from the server (`fn::push::info()`), so there is no
`google-services.json`, no Google services Gradle plugin and no FlutterFire.

## Server

```yaml
push:
  apns:
    teamId: ABCDE12345          # Apple Developer team id
    keyId: XYZ987WVUT           # Keys > APNs key id
    key: { vault: APNS_KEY }    # the .p8 (spky env set APNS_KEY "$(cat AuthKey.p8)")
    bundleIds: [com.example.app]
  fcm:
    serviceAccount: { vault: FCM_SERVICE_ACCOUNT }   # Firebase > Service accounts > new key (JSON)
    android:                    # the four values of google-services.json
      projectId: example
      appId: "1:1234567890:android:abcdef"
      apiKey: AIza...
      senderId: "1234567890"
  rules:
    new-message:
      table: message
      to: recipient
      notification: { title: "{{sender}}", body: "{{text | truncate(120)}}", url: "/m/{{conversation | key}}" }
      native:
        android: { channelId: messages }
```

## Install

```yaml
dependencies:
  spooky_push:
    git: { url: https://github.com/mono424/sp00ky.git, path: packages/spooky_push }
```

**iOS:** in Xcode, Signing & Capabilities: add *Push Notifications* (the
`aps-environment` entitlement) and *Background Modes > Remote notifications*
(silent pushes). Development builds get sandbox tokens, TestFlight and the App
Store production ones; the plugin reads which from the provisioning profile.

**Android:** nothing, as long as no other `com.google.firebase.MESSAGING_EVENT`
service is in the app. FCM delivers to one service only. If the app or another
plugin declares one (`live_activities` does), remove the one you do not need in
`android/app/src/main/AndroidManifest.xml`:

```xml
<manifest xmlns:tools="http://schemas.android.com/tools" ...>
  <application>
    <service android:name="com.istornz.live_activities.LiveActivityFirebaseMessagingService"
             tools:node="remove" />
  </application>
</manifest>
```

or keep yours and forward: `Sp00kyPushPlugin.handleMessage(context, message)`
and `Sp00kyPushPlugin.handleNewToken(context, token)`.

## Use

```dart
final push = Sp00kyPush(db);
await push.start();                         // after runApp

push.onOpened.listen((e) => router.go(e.url ?? '/'));
final launch = await push.initialMessage(); // the tap that started the app

// Settings toggle (a user gesture):
await push.enable(label: 'My phone');
await push.disable();

final devices = await db.push.devices();    // web and native, this one marked
await db.push.test();                       // "Push notifications are on"
```

`start()` keeps the registration right from then on: after sign-in, on
reconnect, when the app returns with a changed permission, when the OS rotates
the token. Sign-out removes this device's row (the choice is remembered for the
next sign-in), and on Android the FCM token is dropped.

A rule without any notification is a silent push: iOS wakes the app briefly
(`content-available`), Android delivers a data message; the plugin wakes the
client so it syncs. Pass `onNudge` to do something else.

`autoRegister: true` registers as soon as the permission is granted. It is off
by default: an app that asks for notification permission for another reason
must not opt users into pushes.
