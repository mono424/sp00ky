package dev.sp00ky.push

import android.Manifest
import android.app.Activity
import android.app.ActivityManager
import android.app.NotificationChannel
import android.app.NotificationManager
import android.app.PendingIntent
import android.content.ContentProvider
import android.content.ContentValues
import android.content.Context
import android.content.Intent
import android.content.SharedPreferences
import android.content.pm.PackageManager
import android.database.Cursor
import android.net.Uri
import android.os.Build
import android.os.Bundle
import android.os.Handler
import android.os.Looper
import android.provider.Settings
import android.util.Log
import androidx.core.app.ActivityCompat
import androidx.core.app.NotificationCompat
import androidx.core.app.NotificationManagerCompat
import androidx.core.content.ContextCompat
import com.google.firebase.FirebaseApp
import com.google.firebase.FirebaseOptions
import com.google.firebase.messaging.FirebaseMessaging
import com.google.firebase.messaging.FirebaseMessagingService
import com.google.firebase.messaging.RemoteMessage
import io.flutter.embedding.engine.plugins.FlutterPlugin
import io.flutter.embedding.engine.plugins.activity.ActivityAware
import io.flutter.embedding.engine.plugins.activity.ActivityPluginBinding
import io.flutter.plugin.common.MethodCall
import io.flutter.plugin.common.MethodChannel
import io.flutter.plugin.common.PluginRegistry
import org.json.JSONObject

private const val TAG = "spooky_push"
private const val PREFS = "dev.sp00ky.push"
private const val KEY_OPTIONS = "firebase_options"
private const val KEY_TOKEN = "token"
private const val KEY_ASKED = "permission_asked"
private const val KEY_DELETE_PENDING = "delete_token_pending"
private const val KEY_FOREGROUND = "foreground"
private const val KEY_SIGNED_IN = "signed_in"
private const val KEY_CHANNEL = "default_channel"
private const val DEFAULT_CHANNEL = "sp00ky_default"
private const val PERMISSION_REQUEST = 0x5b00
/** The data field every sp00ky push carries (the payload, as JSON). */
const val EXTRA_PAYLOAD = "sp00ky"

private fun prefs(context: Context): SharedPreferences =
    context.getSharedPreferences(PREFS, Context.MODE_PRIVATE)

/**
 * Firebase without google-services.json: the client config comes from the
 * sp00ky server (`fn::push::info().android`), is saved, and is applied as the
 * DEFAULT app (FCM only works with the default app) on every process start by
 * [Sp00kyPushInitProvider].
 */
internal object Firebase {
    fun options(json: JSONObject): FirebaseOptions =
        FirebaseOptions.Builder()
            .setProjectId(json.getString("projectId"))
            .setApplicationId(json.getString("appId"))
            .setApiKey(json.getString("apiKey"))
            .setGcmSenderId(json.getString("senderId"))
            .build()

    fun defaultApp(): FirebaseApp? =
        try {
            FirebaseApp.getInstance()
        } catch (e: IllegalStateException) {
            null
        }

    fun apply(context: Context, json: JSONObject): Map<String, Any?> {
        val saved = prefs(context).getString(KEY_OPTIONS, null)
        prefs(context).edit().putString(KEY_OPTIONS, json.toString()).apply()
        val existing = defaultApp()
            ?: return try {
                FirebaseApp.initializeApp(context, options(json))
                mapOf("ready" to true, "restartRequired" to false)
            } catch (e: Exception) {
                mapOf("ready" to false, "error" to (e.message ?: e.toString()))
            }
        val o = existing.options
        if (o.projectId == json.optString("projectId") && o.gcmSenderId == json.optString("senderId")) {
            return mapOf("ready" to true, "restartRequired" to false)
        }
        // Our own options from an earlier config: the new ones apply at the
        // next process start. Anything else is another project's
        // google-services.json, whose tokens sp00ky's service account cannot use.
        val ours = saved != null && JSONObject(saved).optString("projectId") == o.projectId
        return if (ours) {
            mapOf("ready" to true, "restartRequired" to true)
        } else {
            mapOf(
                "ready" to false,
                "conflict" to "the app's default Firebase app is project ${o.projectId}, sp00ky's is ${json.optString("projectId")}",
            )
        }
    }
}

/** Applies the saved Firebase options before any service of the process runs. */
class Sp00kyPushInitProvider : ContentProvider() {
    override fun onCreate(): Boolean {
        val context = context ?: return false
        val saved = prefs(context).getString(KEY_OPTIONS, null) ?: return false
        if (FirebaseApp.getApps(context).isEmpty()) {
            try {
                FirebaseApp.initializeApp(context, Firebase.options(JSONObject(saved)))
            } catch (e: Exception) {
                Log.w(TAG, "saved Firebase options unusable: ${e.message}")
            }
        }
        return false
    }

    override fun query(u: Uri, p: Array<String>?, s: String?, a: Array<String>?, o: String?): Cursor? = null
    override fun getType(uri: Uri): String? = null
    override fun insert(uri: Uri, values: ContentValues?): Uri? = null
    override fun delete(uri: Uri, s: String?, a: Array<String>?): Int = 0
    override fun update(uri: Uri, v: ContentValues?, s: String?, a: Array<String>?): Int = 0
}

/**
 * Receives FCM messages and tokens. Only one MESSAGING_EVENT service of an
 * app gets them: an app with its own (or another plugin's) service removes
 * one in its manifest and forwards to [Sp00kyPushPlugin.handleMessage] /
 * [Sp00kyPushPlugin.handleNewToken].
 */
class Sp00kyMessagingService : FirebaseMessagingService() {
    override fun onNewToken(token: String) = Sp00kyPushPlugin.handleNewToken(applicationContext, token)

    override fun onMessageReceived(message: RemoteMessage) {
        Sp00kyPushPlugin.handleMessage(applicationContext, message)
    }
}

class Sp00kyPushPlugin :
    FlutterPlugin,
    MethodChannel.MethodCallHandler,
    ActivityAware,
    PluginRegistry.NewIntentListener,
    PluginRegistry.RequestPermissionsResultListener {

    companion object {
        @Volatile private var attached: Sp00kyPushPlugin? = null
        private val main = Handler(Looper.getMainLooper())

        @JvmStatic
        fun handleNewToken(context: Context, token: String) {
            prefs(context).edit().putString(KEY_TOKEN, token).apply()
            main.post { attached?.emit("onToken", token) }
        }

        /** `true` when it was a sp00ky push. */
        @JvmStatic
        fun handleMessage(context: Context, message: RemoteMessage): Boolean {
            val data = message.data
            if (!data.containsKey(EXTRA_PAYLOAD)) return false
            val foreground = isForeground()
            val raw = rawOf(message)
            attached?.let { plugin ->
                main.post { plugin.emit("onMessage", mapOf("raw" to raw, "foreground" to foreground)) }
            }
            val n = message.notification ?: return true
            // In the background the system shows notification messages itself.
            val p = prefs(context)
            if (foreground && p.getString(KEY_FOREGROUND, "hide") == "show" && p.getBoolean(KEY_SIGNED_IN, false)) {
                show(context, n, data)
            }
            return true
        }

        private fun isForeground(): Boolean {
            val info = ActivityManager.RunningAppProcessInfo()
            ActivityManager.getMyMemoryState(info)
            return info.importance == ActivityManager.RunningAppProcessInfo.IMPORTANCE_FOREGROUND
        }

        private fun rawOf(message: RemoteMessage): Map<String, Any?> {
            val raw = HashMap<String, Any?>(message.data)
            message.notification?.let { n ->
                raw["notification"] = mapOf(
                    "title" to n.title,
                    "body" to n.body,
                    "tag" to n.tag,
                    "channelId" to n.channelId,
                )
            }
            message.messageId?.let { raw["messageId"] = it }
            raw["sentTime"] = message.sentTime
            return raw
        }

        private fun show(context: Context, n: RemoteMessage.Notification, data: Map<String, String>) {
            if (!canNotify(context)) return
            val launch = context.packageManager.getLaunchIntentForPackage(context.packageName) ?: return
            launch.addFlags(Intent.FLAG_ACTIVITY_NEW_TASK or Intent.FLAG_ACTIVITY_SINGLE_TOP)
            for ((k, v) in data) launch.putExtra(k, v)
            val intent = PendingIntent.getActivity(
                context,
                (System.currentTimeMillis() and 0x7fffffff).toInt(),
                launch,
                PendingIntent.FLAG_IMMUTABLE or PendingIntent.FLAG_UPDATE_CURRENT,
            )
            val channel = n.channelId ?: prefs(context).getString(KEY_CHANNEL, DEFAULT_CHANNEL) ?: DEFAULT_CHANNEL
            val notification = NotificationCompat.Builder(context, channel)
                .setSmallIcon(smallIcon(context))
                .setContentTitle(n.title)
                .setContentText(n.body)
                .setAutoCancel(true)
                .setPriority(NotificationCompat.PRIORITY_HIGH)
                .setContentIntent(intent)
                .build()
            try {
                NotificationManagerCompat.from(context).notify(n.tag, 0, notification)
            } catch (e: SecurityException) {
                Log.w(TAG, "notification not shown: ${e.message}")
            }
        }

        private fun canNotify(context: Context): Boolean {
            if (Build.VERSION.SDK_INT >= 33 &&
                ContextCompat.checkSelfPermission(context, Manifest.permission.POST_NOTIFICATIONS) !=
                PackageManager.PERMISSION_GRANTED
            ) {
                return false
            }
            return NotificationManagerCompat.from(context).areNotificationsEnabled()
        }

        private fun smallIcon(context: Context): Int {
            val meta = try {
                context.packageManager.getApplicationInfo(context.packageName, PackageManager.GET_META_DATA).metaData
            } catch (e: PackageManager.NameNotFoundException) {
                null
            }
            val icon = meta?.getInt("com.google.firebase.messaging.default_notification_icon", 0) ?: 0
            return if (icon != 0) icon else context.applicationInfo.icon
        }
    }

    private lateinit var context: Context
    private lateinit var channel: MethodChannel
    private var activity: Activity? = null
    private var binding: ActivityPluginBinding? = null
    private var ready = false
    private val queued = ArrayList<Pair<String, Any?>>()
    private var initial: Map<String, Any?>? = null
    private var pendingPermission: MethodChannel.Result? = null

    override fun onAttachedToEngine(b: FlutterPlugin.FlutterPluginBinding) {
        context = b.applicationContext
        channel = MethodChannel(b.binaryMessenger, "dev.sp00ky/push")
        channel.setMethodCallHandler(this)
        attached = this
    }

    override fun onDetachedFromEngine(b: FlutterPlugin.FlutterPluginBinding) {
        channel.setMethodCallHandler(null)
        if (attached === this) attached = null
    }

    override fun onAttachedToActivity(b: ActivityPluginBinding) {
        binding = b
        activity = b.activity
        b.addOnNewIntentListener(this)
        b.addRequestPermissionsResultListener(this)
        opened(b.activity.intent)
    }

    override fun onDetachedFromActivityForConfigChanges() = onDetachedFromActivity()

    override fun onReattachedToActivityForConfigChanges(b: ActivityPluginBinding) {
        binding = b
        activity = b.activity
        b.addOnNewIntentListener(this)
        b.addRequestPermissionsResultListener(this)
    }

    override fun onDetachedFromActivity() {
        binding?.removeOnNewIntentListener(this)
        binding?.removeRequestPermissionsResultListener(this)
        binding = null
        activity = null
    }

    override fun onNewIntent(intent: Intent): Boolean {
        opened(intent)
        return false
    }

    /** A tap on a sp00ky notification (ours, or one the system showed). */
    private fun opened(intent: Intent?) {
        val extras = intent?.extras ?: return
        if (!extras.containsKey(EXTRA_PAYLOAD)) return
        val raw = HashMap<String, Any?>()
        for (key in extras.keySet()) {
            val value = extras.get(key)
            if (value is String || value is Number || value is Boolean) raw[key] = value
        }
        // A recreated activity must not replay the tap.
        intent.removeExtra(EXTRA_PAYLOAD)
        emit("onOpened", mapOf("raw" to raw))
    }

    fun emit(method: String, arguments: Any?) {
        if (ready) {
            channel.invokeMethod(method, arguments)
        } else if (method == "onOpened") {
            @Suppress("UNCHECKED_CAST")
            initial = arguments as Map<String, Any?>
        } else {
            queued.add(method to arguments)
        }
    }

    override fun onMethodCall(call: MethodCall, result: MethodChannel.Result) {
        when (call.method) {
            "initialize" -> initialize(call, result)
            "permission" -> result.success(permission())
            "requestPermission" -> requestPermission(result)
            "configureFirebase" -> configureFirebase(call, result)
            "getToken" -> getToken(result)
            "deleteToken" -> deleteToken(result)
            "setSignedIn" -> {
                prefs(context).edit().putBoolean(KEY_SIGNED_IN, call.argument<Boolean>("signedIn") ?: false).apply()
                result.success(null)
            }
            "createChannel" -> {
                createChannel(call.arguments as? Map<*, *>)
                result.success(null)
            }
            "openSettings" -> result.success(openSettings())
            "setBadge", "completeBackground" -> result.success(null)
            else -> result.notImplemented()
        }
    }

    private fun initialize(call: MethodCall, result: MethodChannel.Result) {
        val channelArgs = call.argument<Map<String, Any?>>("channel")
        prefs(context).edit()
            .putString(KEY_FOREGROUND, call.argument<String>("foreground") ?: "hide")
            .putBoolean(KEY_SIGNED_IN, call.argument<Boolean>("signedIn") ?: false)
            .putString(KEY_CHANNEL, channelArgs?.get("id") as? String ?: DEFAULT_CHANNEL)
            .apply()
        createChannel(channelArgs)
        val firebaseReady = FirebaseApp.getApps(context).isNotEmpty()
        val out = hashMapOf<String, Any?>(
            "platform" to "android",
            "appId" to context.packageName,
            "permission" to permission(),
            "model" to Build.MODEL,
            "osVersion" to Build.VERSION.RELEASE,
            "firebaseReady" to firebaseReady,
            "initial" to initial,
        )
        if (firebaseReady) out["token"] = prefs(context).getString(KEY_TOKEN, null)
        initial = null
        ready = true
        result.success(out)
        val waiting = ArrayList(queued)
        queued.clear()
        for ((method, arguments) in waiting) channel.invokeMethod(method, arguments)
        if (firebaseReady && prefs(context).getBoolean(KEY_DELETE_PENDING, false)) deleteToken(null)
    }

    private fun permission(): String {
        val enabled = NotificationManagerCompat.from(context).areNotificationsEnabled()
        if (Build.VERSION.SDK_INT < 33) return if (enabled) "granted" else "denied"
        if (ContextCompat.checkSelfPermission(context, Manifest.permission.POST_NOTIFICATIONS) ==
            PackageManager.PERMISSION_GRANTED
        ) {
            return if (enabled) "granted" else "denied"
        }
        val act = activity
        return when {
            !prefs(context).getBoolean(KEY_ASKED, false) -> "notDetermined"
            // Android allows asking again after a first denial.
            act != null && ActivityCompat.shouldShowRequestPermissionRationale(
                act, Manifest.permission.POST_NOTIFICATIONS,
            ) -> "notDetermined"
            else -> "denied"
        }
    }

    private fun requestPermission(result: MethodChannel.Result) {
        val act = activity
        if (Build.VERSION.SDK_INT < 33 || act == null || permission() != "notDetermined") {
            result.success(permission())
            return
        }
        pendingPermission?.success(permission())
        pendingPermission = result
        prefs(context).edit().putBoolean(KEY_ASKED, true).apply()
        ActivityCompat.requestPermissions(act, arrayOf(Manifest.permission.POST_NOTIFICATIONS), PERMISSION_REQUEST)
    }

    override fun onRequestPermissionsResult(
        requestCode: Int,
        permissions: Array<out String>,
        grantResults: IntArray,
    ): Boolean {
        if (requestCode != PERMISSION_REQUEST) return false
        pendingPermission?.success(permission())
        pendingPermission = null
        return true
    }

    private fun configureFirebase(call: MethodCall, result: MethodChannel.Result) {
        val args = call.arguments as? Map<*, *>
        if (args == null) {
            result.success(mapOf("ready" to false, "error" to "no options"))
            return
        }
        val json = JSONObject()
        for (key in listOf("projectId", "appId", "apiKey", "senderId")) json.put(key, args[key]?.toString() ?: "")
        result.success(Firebase.apply(context, json))
    }

    private fun getToken(result: MethodChannel.Result) {
        if (FirebaseApp.getApps(context).isEmpty()) {
            result.error("no-firebase", "configureFirebase first", null)
            return
        }
        FirebaseMessaging.getInstance().token.addOnCompleteListener { task ->
            val token = if (task.isSuccessful) task.result else null
            if (token != null) {
                prefs(context).edit().putString(KEY_TOKEN, token).apply()
                result.success(token)
            } else {
                result.error("no-token", task.exception?.message ?: "FCM gave no token", null)
            }
        }
    }

    /** Sign-out: a new token next time, so the old one stops getting pushes. */
    private fun deleteToken(result: MethodChannel.Result?) {
        val p = prefs(context)
        if (FirebaseApp.getApps(context).isEmpty()) {
            p.edit().putBoolean(KEY_DELETE_PENDING, true).apply()
            result?.success(null)
            return
        }
        FirebaseMessaging.getInstance().deleteToken().addOnCompleteListener { task ->
            val edit = p.edit().putBoolean(KEY_DELETE_PENDING, !task.isSuccessful)
            if (task.isSuccessful) edit.remove(KEY_TOKEN)
            edit.apply()
            result?.success(null)
        }
    }

    private fun createChannel(args: Map<*, *>?) {
        if (Build.VERSION.SDK_INT < 26 || args == null) return
        val id = args["id"] as? String ?: return
        val name = args["name"] as? String ?: id
        val importance = (args["importance"] as? Number)?.toInt() ?: NotificationManager.IMPORTANCE_HIGH
        val channel = NotificationChannel(id, name, importance)
        (args["description"] as? String)?.let { channel.description = it }
        context.getSystemService(NotificationManager::class.java)?.createNotificationChannel(channel)
    }

    private fun openSettings(): Boolean {
        val intent = if (Build.VERSION.SDK_INT >= 26) {
            Intent(Settings.ACTION_APP_NOTIFICATION_SETTINGS)
                .putExtra(Settings.EXTRA_APP_PACKAGE, context.packageName)
        } else {
            Intent(Settings.ACTION_APPLICATION_DETAILS_SETTINGS, Uri.parse("package:${context.packageName}"))
        }
        intent.addFlags(Intent.FLAG_ACTIVITY_NEW_TASK)
        return try {
            context.startActivity(intent)
            true
        } catch (e: Exception) {
            false
        }
    }
}
