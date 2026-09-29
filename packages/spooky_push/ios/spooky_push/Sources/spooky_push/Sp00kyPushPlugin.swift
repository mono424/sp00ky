import Flutter
import UIKit
import UserNotifications

/// iOS side of spooky_push: APNs registration, permission, notifications the
/// app receives and the ones the user taps. No Firebase: the raw APNs device
/// token goes to the server, which talks to Apple itself.
///
/// Coexistence with other plugins: the notification-center delegate is
/// installed at registration and chains to a delegate that was already set;
/// when the app delegate (FlutterAppDelegate) is the delegate, Flutter hands
/// every plugin the same completion handler, so this plugin only ever
/// completes notifications that carry a `sp00ky` payload.
public final class Sp00kyPushPlugin: NSObject, FlutterPlugin, FlutterSceneLifeCycleDelegate,
  UNUserNotificationCenterDelegate
{
  private static var shared: Sp00kyPushPlugin?

  private let channel: FlutterMethodChannel
  /// Dart called `initialize`; before that, events wait here.
  private var dartReady = false
  private var queued: [(String, Any)] = []
  /// The tap that launched the app, until Dart asks for it.
  private var initial: [String: Any]?
  private var seenResponses = Set<String>()
  private var showInForeground = false
  private var signedIn = false
  private var environmentOverride: String?
  private var token: String?
  private var tokenWaiters: [(Any?) -> Void] = []
  private var backgroundTasks: [String: (UIBackgroundFetchResult) -> Void] = [:]
  /// A notification-center delegate that was there before us.
  private var chained: UNUserNotificationCenterDelegate?

  init(channel: FlutterMethodChannel) {
    self.channel = channel
    super.init()
  }

  public static func register(with registrar: FlutterPluginRegistrar) {
    let channel = FlutterMethodChannel(name: "dev.sp00ky/push", binaryMessenger: registrar.messenger())
    let instance = Sp00kyPushPlugin(channel: channel)
    shared = instance
    registrar.addMethodCallDelegate(instance, channel: channel)
    registrar.addApplicationDelegate(instance)
    // UIScene apps (the implicit engine) register plugins after launch:
    // a cold-start tap only reaches us through the scene connection.
    registrar.addSceneDelegate(instance)
    instance.installNotificationDelegate()
  }

  /// For apps that route notification responses themselves (their own
  /// scene delegate or notification-center delegate). `true` when it was a
  /// sp00ky notification.
  @discardableResult
  public static func handle(response: UNNotificationResponse) -> Bool {
    shared?.opened(response) ?? false
  }

  private func installNotificationDelegate() {
    let center = UNUserNotificationCenter.current()
    guard let current = center.delegate else {
      center.delegate = self
      return
    }
    if current === self { return }
    // FlutterAppDelegate forwards to plugins registered as application
    // delegates: nothing to install.
    if let app = UIApplication.shared.delegate, (current as AnyObject) === (app as AnyObject) {
      return
    }
    chained = current
    center.delegate = self
  }

  // MARK: - Dart -> native

  public func handle(_ call: FlutterMethodCall, result: @escaping FlutterResult) {
    let args = call.arguments as? [String: Any] ?? [:]
    switch call.method {
    case "initialize":
      initialize(args, result)
    case "permission":
      Self.permission { result($0) }
    case "requestPermission":
      requestPermission(provisional: args["provisional"] as? Bool ?? false) { result($0) }
    case "getToken":
      getToken(timeoutMs: args["timeoutMs"] as? Int ?? 10_000, result)
    case "completeBackground":
      complete(args["id"] as? String, args["result"] as? String)
      result(nil)
    case "setSignedIn":
      signedIn = args["signedIn"] as? Bool ?? false
      result(nil)
    case "setBadge":
      setBadge(args["count"] as? Int ?? 0)
      result(nil)
    case "openSettings":
      openSettings(result)
    case "deleteToken", "createChannel", "configureFirebase":
      // Android only.
      result(nil)
    default:
      result(FlutterMethodNotImplemented)
    }
  }

  private func initialize(_ args: [String: Any], _ result: @escaping FlutterResult) {
    showInForeground = (args["foreground"] as? String) == "show"
    signedIn = args["signedIn"] as? Bool ?? false
    environmentOverride = args["apnsEnvironment"] as? String
    Self.permission { permission in
      if permission == "granted" || permission == "provisional" {
        // Apple re-delivers the (possibly new) token on every launch.
        UIApplication.shared.registerForRemoteNotifications()
      }
      var out: [String: Any] = [
        "platform": "ios",
        "appId": Bundle.main.bundleIdentifier ?? "",
        "permission": permission,
        "model": Self.model(),
        "osVersion": UIDevice.current.systemVersion,
        "environment": self.environmentOverride ?? Self.apsEnvironment(),
      ]
      if let t = self.token { out["token"] = t }
      if let i = self.initial {
        out["initial"] = i
        self.initial = nil
      }
      self.dartReady = true
      result(out)
      let waiting = self.queued
      self.queued = []
      for (method, arguments) in waiting {
        self.channel.invokeMethod(method, arguments: arguments)
      }
    }
  }

  private func send(_ method: String, _ arguments: Any) {
    DispatchQueue.main.async {
      if self.dartReady {
        self.channel.invokeMethod(method, arguments: arguments)
      } else {
        self.queued.append((method, arguments))
      }
    }
  }

  // MARK: - Permission and token

  static func permission(_ done: @escaping (String) -> Void) {
    UNUserNotificationCenter.current().getNotificationSettings { settings in
      // By raw value: `.ephemeral` (4, App Clips) is iOS 14+.
      let value: String
      switch settings.authorizationStatus.rawValue {
      case UNAuthorizationStatus.authorized.rawValue, 4: value = "granted"
      case UNAuthorizationStatus.provisional.rawValue: value = "provisional"
      case UNAuthorizationStatus.denied.rawValue: value = "denied"
      default: value = "notDetermined"
      }
      DispatchQueue.main.async { done(value) }
    }
  }

  private func requestPermission(provisional: Bool, _ done: @escaping (String) -> Void) {
    var options: UNAuthorizationOptions = [.alert, .badge, .sound]
    if provisional { options.insert(.provisional) }
    UNUserNotificationCenter.current().requestAuthorization(options: options) { granted, _ in
      DispatchQueue.main.async {
        if granted { UIApplication.shared.registerForRemoteNotifications() }
        Self.permission(done)
      }
    }
  }

  private func getToken(timeoutMs: Int, _ result: @escaping FlutterResult) {
    if let t = token {
      result(t)
      return
    }
    var answered = false
    let answer: (Any?) -> Void = { value in
      if answered { return }
      answered = true
      result(value)
    }
    tokenWaiters.append(answer)
    UIApplication.shared.registerForRemoteNotifications()
    DispatchQueue.main.asyncAfter(deadline: .now() + .milliseconds(timeoutMs)) {
      answer(FlutterError(code: "no-token", message: "APNs gave no token in time", details: nil))
    }
  }

  public func application(
    _ application: UIApplication, didRegisterForRemoteNotificationsWithDeviceToken deviceToken: Data
  ) {
    let t = deviceToken.map { String(format: "%02x", $0) }.joined()
    let changed = t != token
    token = t
    let waiters = tokenWaiters
    tokenWaiters = []
    waiters.forEach { $0(t) }
    if changed { send("onToken", t) }
  }

  public func application(
    _ application: UIApplication, didFailToRegisterForRemoteNotificationsWithError error: Error
  ) {
    let waiters = tokenWaiters
    tokenWaiters = []
    let failure = FlutterError(code: "no-token", message: error.localizedDescription, details: nil)
    waiters.forEach { $0(failure) }
    send("onTokenError", error.localizedDescription)
  }

  // MARK: - Receiving

  private static func isOurs(_ userInfo: [AnyHashable: Any]) -> Bool {
    userInfo["sp00ky"] != nil
  }

  /// A silent push (`content-available`) woke the app. Dart refreshes and
  /// answers `completeBackground`; iOS allows ~30 s, we give up at 25.
  public func application(
    _ application: UIApplication, didReceiveRemoteNotification userInfo: [AnyHashable: Any],
    fetchCompletionHandler completionHandler: @escaping (UIBackgroundFetchResult) -> Void
  ) -> Bool {
    guard Self.isOurs(userInfo) else { return false }
    let id = UUID().uuidString
    backgroundTasks[id] = completionHandler
    send(
      "onMessage",
      [
        "raw": Self.plist(userInfo),
        "foreground": application.applicationState == .active,
        "id": id,
      ] as [String: Any])
    DispatchQueue.main.asyncAfter(deadline: .now() + 25) { self.complete(id, "noData") }
    return true
  }

  private func complete(_ id: String?, _ result: String?) {
    guard let id, let done = backgroundTasks.removeValue(forKey: id) else { return }
    switch result {
    case "newData": done(.newData)
    case "failed": done(.failed)
    default: done(.noData)
    }
  }

  public func userNotificationCenter(
    _ center: UNUserNotificationCenter, willPresent notification: UNNotification,
    withCompletionHandler completionHandler: @escaping (UNNotificationPresentationOptions) -> Void
  ) {
    let userInfo = notification.request.content.userInfo
    guard Self.isOurs(userInfo) else {
      // Only the actual delegate answers for other notifications; under
      // FlutterAppDelegate forwarding another plugin owns the handler.
      guard center.delegate === self else { return }
      if let other = chained,
        other.responds(
          to: #selector(
            UNUserNotificationCenterDelegate.userNotificationCenter(_:willPresent:withCompletionHandler:)))
      {
        other.userNotificationCenter?(center, willPresent: notification, withCompletionHandler: completionHandler)
      } else {
        completionHandler([])
      }
      return
    }
    send("onMessage", ["raw": Self.plist(userInfo), "foreground": true] as [String: Any])
    // Never show the previous user's pushes on a signed-out device.
    guard showInForeground && signedIn else {
      completionHandler([])
      return
    }
    if #available(iOS 14.0, *) {
      completionHandler([.banner, .list, .sound, .badge])
    } else {
      completionHandler([.alert, .sound, .badge])
    }
  }

  public func userNotificationCenter(
    _ center: UNUserNotificationCenter, didReceive response: UNNotificationResponse,
    withCompletionHandler completionHandler: @escaping () -> Void
  ) {
    if opened(response) {
      completionHandler()
      return
    }
    guard center.delegate === self else { return }
    if let other = chained,
      other.responds(
        to: #selector(
          UNUserNotificationCenterDelegate.userNotificationCenter(_:didReceive:withCompletionHandler:)))
    {
      other.userNotificationCenter?(center, didReceive: response, withCompletionHandler: completionHandler)
    } else {
      completionHandler()
    }
  }

  public func userNotificationCenter(
    _ center: UNUserNotificationCenter, openSettingsFor notification: UNNotification?
  ) {
    guard center.delegate === self else { return }
    chained?.userNotificationCenter?(center, openSettingsFor: notification)
  }

  /// A tap: to Dart, or kept as the launch tap until Dart is up. The same
  /// response can arrive twice (scene connection and delegate).
  @discardableResult
  private func opened(_ response: UNNotificationResponse) -> Bool {
    let notification = response.notification
    let userInfo = notification.request.content.userInfo
    guard Self.isOurs(userInfo) else { return false }
    if response.actionIdentifier == UNNotificationDismissActionIdentifier { return true }
    guard seenResponses.insert(notification.request.identifier).inserted else { return true }
    var event: [String: Any] = ["raw": Self.plist(userInfo)]
    if response.actionIdentifier != UNNotificationDefaultActionIdentifier {
      event["action"] = response.actionIdentifier
    }
    DispatchQueue.main.async {
      if self.dartReady {
        self.channel.invokeMethod("onOpened", arguments: event)
      } else {
        self.initial = event
      }
    }
    return true
  }

  // MARK: - Launch

  public func application(
    _ application: UIApplication, didFinishLaunchingWithOptions launchOptions: [AnyHashable: Any] = [:]
  ) -> Bool {
    if let userInfo = launchOptions[UIApplication.LaunchOptionsKey.remoteNotification] as? [AnyHashable: Any],
      Self.isOurs(userInfo), initial == nil
    {
      initial = ["raw": Self.plist(userInfo)]
    }
    return true
  }

  /// Returns false: the connection options stay available to others.
  public func scene(
    _ scene: UIScene, willConnectTo session: UISceneSession, options connectionOptions: UIScene.ConnectionOptions?
  ) -> Bool {
    if let response = connectionOptions?.notificationResponse {
      opened(response)
    }
    return false
  }

  // MARK: - Misc

  private func setBadge(_ count: Int) {
    if #available(iOS 16.0, *) {
      UNUserNotificationCenter.current().setBadgeCount(count)
    } else {
      UIApplication.shared.applicationIconBadgeNumber = count
    }
  }

  private func openSettings(_ result: @escaping FlutterResult) {
    var link = UIApplication.openSettingsURLString
    if #available(iOS 16.0, *) { link = UIApplication.openNotificationSettingsURLString }
    guard let url = URL(string: link) else {
      result(false)
      return
    }
    UIApplication.shared.open(url, options: [:]) { result($0) }
  }

  static func model() -> String {
    var info = utsname()
    uname(&info)
    let machine = withUnsafeBytes(of: &info.machine) { raw in
      String(decoding: raw.prefix(while: { $0 != 0 }), as: UTF8.self)
    }
    return machine.isEmpty ? UIDevice.current.model : machine
  }

  /// Which APNs host this build's token belongs to: the simulator and
  /// development-signed builds use sandbox; TestFlight and the App Store have
  /// no embedded profile and use production.
  static func apsEnvironment() -> String {
    #if targetEnvironment(simulator)
      return "sandbox"
    #else
      guard let url = Bundle.main.url(forResource: "embedded", withExtension: "mobileprovision"),
        let data = try? Data(contentsOf: url)
      else { return "production" }
      return apsEnvironment(profile: data) == "development" ? "sandbox" : "production"
    #endif
  }

  /// `Entitlements.aps-environment` of a provisioning profile (a CMS
  /// envelope around an XML plist).
  static func apsEnvironment(profile data: Data) -> String? {
    guard let text = String(data: data, encoding: .isoLatin1),
      let start = text.range(of: "<?xml"),
      let end = text.range(of: "</plist>", range: start.upperBound..<text.endIndex),
      let xml = String(text[start.lowerBound..<end.upperBound]).data(using: .isoLatin1),
      let plist = try? PropertyListSerialization.propertyList(from: xml, format: nil) as? [String: Any],
      let entitlements = plist["Entitlements"] as? [String: Any]
    else { return nil }
    return entitlements["aps-environment"] as? String
  }

  /// `userInfo` as the standard codec takes it: string keys, no dates.
  static func plist(_ value: Any) -> Any {
    switch value {
    case let dict as [AnyHashable: Any]:
      var out: [String: Any] = [:]
      for (key, item) in dict { out[String(describing: key.base)] = plist(item) }
      return out
    case let list as [Any]:
      return list.map(plist)
    case let date as Date:
      return ISO8601DateFormatter().string(from: date)
    case let data as Data:
      return FlutterStandardTypedData(bytes: data)
    default:
      return value
    }
  }
}
