Pod::Spec.new do |s|
  s.name             = 'spooky_push'
  s.version          = '0.0.1'
  s.summary          = 'Native push (APNs) for sp00ky apps.'
  s.description      = 'Permission, APNs device token, received and tapped notifications for spooky_push.'
  s.homepage         = 'https://sp00ky.cloud'
  s.license          = { :type => 'MIT' }
  s.author           = { 'sp00ky' => 'info@sp00ky.cloud' }
  s.source           = { :path => '.' }
  s.source_files     = 'spooky_push/Sources/spooky_push/**/*.swift'
  s.dependency 'Flutter'
  s.platform         = :ios, '13.0'
  s.pod_target_xcconfig = { 'DEFINES_MODULE' => 'YES', 'EXCLUDED_ARCHS[sdk=iphonesimulator*]' => 'i386' }
  s.swift_version    = '5.0'
end
