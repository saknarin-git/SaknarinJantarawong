/**
 * ระบบจัดการเงินกู้ บ้านพิตำ V.3.2 (Full Backend + API)
 */

var GOOGLE_SHEETS_DB_ID = String(PropertiesService.getScriptProperties().getProperty('GOOGLE_SHEETS_DB_ID') || '').trim();
var EXECUTION_SPREADSHEET_INSTANCE_ = null;
var EXECUTION_SETTINGS_MAP_CACHE_ = {};
var RUNTIME_DB_READY_CACHE_TTL_SECONDS = 600;
var REQUIRED_TRANSACTION_HEADERS_ = [
  'รหัสอ้างอิง', 'วัน/เดือน/ปี (พ.ศ.)', 'เลขที่สัญญา', 'รหัสสมาชิก',
  'ชำระเงินต้น', 'ชำระดอกเบี้ย', 'ยอดคงเหลือ', 'หมายเหตุ',
  'ผู้ทำรายการ', 'ชื่อ-สกุล', 'สถานะการทำรายการ',
  'จำนวนงวดดอกที่ชำระ', 'ค้างดอกก่อนรับชำระ', 'ค้างดอกหลังรับชำระ'
];
var ACTIVE_API_REQUEST_CONTEXT_ = null;

function getDatabaseSpreadsheet_() {
  if (EXECUTION_SPREADSHEET_INSTANCE_) return EXECUTION_SPREADSHEET_INSTANCE_;
  EXECUTION_SPREADSHEET_INSTANCE_ = GOOGLE_SHEETS_DB_ID
    ? SpreadsheetApp.openById(GOOGLE_SHEETS_DB_ID)
    : SpreadsheetApp.getActiveSpreadsheet();
  return EXECUTION_SPREADSHEET_INSTANCE_;
}

function doGet() {
  return HtmlService.createHtmlOutputFromFile('Index')
    .setTitle('ระบบจัดการเงินกู้ บ้านพิตำ')
    .setXFrameOptionsMode(HtmlService.XFrameOptionsMode.DEFAULT)
    .addMetaTag('viewport', 'width=device-width, initial-scale=1');
}

function parseApiRequestPayload_(e) {
  var rawPayload = '';
  try {
    rawPayload = String((e && e.postData && e.postData.contents) || '').trim();
  } catch (error) {
    rawPayload = '';
  }

  if (!rawPayload && e && e.parameter && e.parameter.payload) {
    rawPayload = String(e.parameter.payload || '').trim();
  }

  if (!rawPayload) return {};

  try {
    return JSON.parse(rawPayload);
  } catch (error) {
    return { raw: rawPayload };
  }
}

function createJsonApiResponse_(payload) {
  return ContentService
    .createTextOutput(JSON.stringify(payload == null ? {} : payload))
    .setMimeType(ContentService.MimeType.JSON);
}

function isAllowedApiMethodName_(methodName) {
  var normalizedMethodName = String(methodName || '').trim();
  if (!normalizedMethodName) return false;
  if (!/^[A-Za-z0-9_]+$/.test(normalizedMethodName)) return false;
  if (normalizedMethodName === 'doGet' || normalizedMethodName === 'doPost') return false;
  if (normalizedMethodName.charAt(0) === '_' || normalizedMethodName.charAt(normalizedMethodName.length - 1) === '_') return false;
  return typeof this[normalizedMethodName] === 'function';
}

function doPost(e) {
  var previousRequestContext = ACTIVE_API_REQUEST_CONTEXT_;
  try {
    var payload = parseApiRequestPayload_(e);
    var methodName = String(payload.method || payload.methodName || '').trim();
    var args = Array.isArray(payload.args) ? payload.args : [];
    var sessionToken = String(payload.sessionToken || '').trim();

    if (!methodName) {
      return createJsonApiResponse_({ status: 'Error', message: 'ไม่ได้ระบุชื่อเมธอดของ backend' });
    }

    if (!isAllowedApiMethodName_(methodName)) {
      return createJsonApiResponse_({ status: 'Error', message: 'เมธอดนี้ไม่อนุญาตให้เรียกผ่าน API' });
    }

    ACTIVE_API_REQUEST_CONTEXT_ = {
      sessionToken: sessionToken,
      sessionData: getApiSessionFromToken_(sessionToken)
    };

    var backendFn = this[methodName];
    var result = backendFn.apply(null, args);
    return createJsonApiResponse_(result);
  } catch (error) {
    return createJsonApiResponse_({
      status: 'Error',
      message: error && error.message ? error.message : String(error || 'เกิดข้อผิดพลาดไม่ทราบสาเหตุ')
    });
  } finally {
    ACTIVE_API_REQUEST_CONTEXT_ = previousRequestContext;
  }
}

var AUTH_SESSION_TTL_SECONDS = 21600;
var MAX_LOGIN_ATTEMPTS = 5;
var LOGIN_LOCKOUT_SECONDS = 900;
var USER_AUTHZ_CACHE_TTL_SECONDS = 120;
var PASSWORD_RESET_REQUEST_COOLDOWN_SECONDS = 60;
var MAX_PASSWORD_RESET_VERIFY_ATTEMPTS = 5;
var PASSWORD_RESET_VERIFY_LOCKOUT_SECONDS = 900;

function getSessionIdentityKey_() {
  try {
    var temporaryKey = String(Session.getTemporaryActiveUserKey() || '').trim();
    if (temporaryKey) return 'tmp:' + temporaryKey;
  } catch (e) {}

  try {
    var activeEmail = String(Session.getActiveUser().getEmail() || '').trim().toLowerCase();
    if (activeEmail) return 'email:' + activeEmail;
  } catch (e) {}

  return '';
}

function getAuthSessionCacheKey_() {
  var identityKey = getSessionIdentityKey_();
  return identityKey ? ('auth_session_' + identityKey) : '';
}

function getLoginAttemptCacheKey_(username) {
  return 'login_attempts_' + String(username || '').trim().toLowerCase();
}

function getLoginLockCacheKey_(username) {
  return 'login_lock_' + String(username || '').trim().toLowerCase();
}

function normalizeAccountIdentifierForCache_(identifier) {
  var normalizedEmail = normalizeEmail_(identifier);
  if (normalizedEmail) return normalizedEmail;
  var normalizedUsername = normalizeUsername_(identifier);
  if (normalizedUsername) return normalizedUsername;
  return String(identifier || '').trim().toLowerCase();
}

function getPasswordResetRequestCacheKey_(identifier) {
  var normalizedIdentifier = normalizeAccountIdentifierForCache_(identifier);
  return normalizedIdentifier ? ('password_reset_request_' + normalizedIdentifier) : '';
}

function getPasswordResetVerifyAttemptCacheKey_(identifier) {
  var normalizedIdentifier = normalizeAccountIdentifierForCache_(identifier);
  return normalizedIdentifier ? ('password_reset_verify_attempt_' + normalizedIdentifier) : '';
}

function getPasswordResetVerifyLockCacheKey_(identifier) {
  var normalizedIdentifier = normalizeAccountIdentifierForCache_(identifier);
  return normalizedIdentifier ? ('password_reset_verify_lock_' + normalizedIdentifier) : '';
}

function putTimedScriptCacheValue_(cacheKey, seconds) {
  if (!cacheKey || !seconds || seconds <= 0) return;
  try {
    CacheService.getScriptCache().put(cacheKey, String(Date.now() + (seconds * 1000)), seconds);
  } catch (e) {}
}

function getTimedScriptCacheRemainingSeconds_(cacheKey) {
  if (!cacheKey) return 0;
  try {
    var rawValue = CacheService.getScriptCache().get(cacheKey);
    if (!rawValue) return 0;
    var expireAt = Number(rawValue) || 0;
    var remainingSeconds = Math.ceil((expireAt - Date.now()) / 1000);
    if (remainingSeconds <= 0) {
      CacheService.getScriptCache().remove(cacheKey);
      return 0;
    }
    return remainingSeconds;
  } catch (e) {
    return 0;
  }
}

function getPasswordResetRequestThrottleRemainingSeconds_(identifier) {
  return getTimedScriptCacheRemainingSeconds_(getPasswordResetRequestCacheKey_(identifier));
}

function setPasswordResetRequestThrottle_(identifier) {
  putTimedScriptCacheValue_(getPasswordResetRequestCacheKey_(identifier), PASSWORD_RESET_REQUEST_COOLDOWN_SECONDS);
}

function getPasswordResetVerifyLockRemainingSeconds_(identifier) {
  return getTimedScriptCacheRemainingSeconds_(getPasswordResetVerifyLockCacheKey_(identifier));
}

function registerPasswordResetVerifyFailure_(identifier) {
  var attemptKey = getPasswordResetVerifyAttemptCacheKey_(identifier);
  var lockKey = getPasswordResetVerifyLockCacheKey_(identifier);
  if (!attemptKey || !lockKey) return { locked: false, attempts: 0 };

  try {
    var cache = CacheService.getScriptCache();
    var currentAttempts = Number(cache.get(attemptKey) || 0) + 1;
    if (currentAttempts >= MAX_PASSWORD_RESET_VERIFY_ATTEMPTS) {
      putTimedScriptCacheValue_(lockKey, PASSWORD_RESET_VERIFY_LOCKOUT_SECONDS);
      cache.remove(attemptKey);
      return { locked: true, attempts: currentAttempts };
    }
    cache.put(attemptKey, String(currentAttempts), PASSWORD_RESET_VERIFY_LOCKOUT_SECONDS);
    return { locked: false, attempts: currentAttempts };
  } catch (e) {
    return { locked: false, attempts: 0 };
  }
}

function clearPasswordResetVerifyFailures_(identifier) {
  try {
    var cache = CacheService.getScriptCache();
    var attemptKey = getPasswordResetVerifyAttemptCacheKey_(identifier);
    var lockKey = getPasswordResetVerifyLockCacheKey_(identifier);
    if (attemptKey) cache.remove(attemptKey);
    if (lockKey) cache.remove(lockKey);
  } catch (e) {}
}

function getUserAuthorizationCacheKey_(username) {
  var normalizedUsername = normalizeUsername_(username);
  return normalizedUsername ? ('user_authz_' + normalizedUsername) : '';
}

function invalidateUserAuthorizationCache_(username) {
  var cacheKey = getUserAuthorizationCacheKey_(username);
  if (!cacheKey) return;
  try {
    CacheService.getScriptCache().remove(cacheKey);
  } catch (e) {}
}

function getUserAuthorizationSnapshot_(username) {
  var cacheKey = getUserAuthorizationCacheKey_(username);
  if (!cacheKey) return null;

  try {
    var rawCache = CacheService.getScriptCache().get(cacheKey);
    if (rawCache) {
      var parsedCache = JSON.parse(rawCache);
      if (parsedCache && parsedCache.username) return parsedCache;
    }
  } catch (e) {}

  var ss = getDatabaseSpreadsheet_();
  var usersSheet = ensureUsersSheet_(ss);
  var userRecord = findUserByUsernameInternal_(usersSheet, username);
  if (!userRecord || !userRecord.username) {
    invalidateUserAuthorizationCache_(username);
    return null;
  }

  var snapshot = {
    userId: String(userRecord.userId || '').trim(),
    username: String(userRecord.username || '').trim(),
    fullName: String(userRecord.fullName || buildFullNameFromParts_(userRecord.prefix, userRecord.firstName, userRecord.lastName) || userRecord.username || '').trim(),
    role: normalizeUserRole_(userRecord.role),
    status: normalizeUserStatus_(userRecord.status),
    emailVerifiedAt: String(userRecord.emailVerifiedAt || '').trim(),
    hasPin: !!String(userRecord.pinHash || '').trim(),
    updatedAt: String(userRecord.updatedAt || '').trim(),
    permissionsJson: String(userRecord.permissionsJson || '').trim(),
    permissions: getEffectivePermissionsForUserRecord_(userRecord),
    permissionsMap: getEffectivePermissionMapForUserRecord_(userRecord)
  };

  try {
    CacheService.getScriptCache().put(cacheKey, JSON.stringify(snapshot), USER_AUTHZ_CACHE_TTL_SECONDS);
  } catch (e) {}

  return snapshot;
}

function createAuthenticatedSession_(userInfo) {
  var normalized = (typeof userInfo === 'object' && userInfo !== null) ? userInfo : { username: userInfo };
  var sessionData = {
    userId: String(normalized.userId || '').trim(),
    username: String(normalized.username || 'admin').trim() || 'admin',
    fullName: String(normalized.fullName || normalized.username || 'Admin').trim() || 'Admin',
    role: String(normalized.role || 'admin').trim() || 'admin',
    issuedAt: new Date().toISOString()
  };

  CacheService.getUserCache().put('auth_session_current', JSON.stringify(sessionData), AUTH_SESSION_TTL_SECONDS);

  var cacheKey = getAuthSessionCacheKey_();
  if (cacheKey) {
    CacheService.getScriptCache().put(cacheKey, JSON.stringify(sessionData), AUTH_SESSION_TTL_SECONDS);
  }

  if (ACTIVE_API_REQUEST_CONTEXT_) {
    ACTIVE_API_REQUEST_CONTEXT_.sessionData = sessionData;
  }

  return sessionData;
}

function getApiSessionFromRequestContext_() {
  if (!ACTIVE_API_REQUEST_CONTEXT_ || !ACTIVE_API_REQUEST_CONTEXT_.sessionData) return null;
  var sessionData = ACTIVE_API_REQUEST_CONTEXT_.sessionData;
  return sessionData && sessionData.username ? sessionData : null;
}

function getApiSessionCacheKey_(sessionToken) {
  var normalizedToken = String(sessionToken || '').trim();
  return normalizedToken ? ('auth_api_session_' + normalizedToken) : '';
}

function createApiSessionToken_(sessionData) {
  if (!sessionData || !sessionData.username) return '';
  var sessionToken = Utilities.getUuid() + Utilities.getUuid().replace(/-/g, '');
  touchApiSessionToken_(sessionToken, sessionData);
  return sessionToken;
}

function getApiSessionFromToken_(sessionToken) {
  var cacheKey = getApiSessionCacheKey_(sessionToken);
  if (!cacheKey) return null;

  try {
    var rawCache = CacheService.getScriptCache().get(cacheKey);
    if (!rawCache) return null;
    var parsedCache = JSON.parse(rawCache);
    if (parsedCache && parsedCache.username) return parsedCache;
  } catch (e) {}

  return null;
}

function touchApiSessionToken_(sessionToken, sessionData) {
  var cacheKey = getApiSessionCacheKey_(sessionToken);
  if (!cacheKey || !sessionData || !sessionData.username) return null;

  var refreshedSession = {
    userId: String(sessionData.userId || '').trim(),
    username: String(sessionData.username || '').trim(),
    fullName: String(sessionData.fullName || sessionData.username || '').trim(),
    role: String(sessionData.role || 'staff').trim() || 'staff',
    issuedAt: String(sessionData.issuedAt || new Date().toISOString()),
    lastSeenAt: new Date().toISOString()
  };

  try {
    CacheService.getScriptCache().put(cacheKey, JSON.stringify(refreshedSession), AUTH_SESSION_TTL_SECONDS);
  } catch (e) {}

  if (ACTIVE_API_REQUEST_CONTEXT_ && String(ACTIVE_API_REQUEST_CONTEXT_.sessionToken || '').trim() === String(sessionToken || '').trim()) {
    ACTIVE_API_REQUEST_CONTEXT_.sessionData = refreshedSession;
  }

  return refreshedSession;
}

function clearApiSessionToken_(sessionToken) {
  var cacheKey = getApiSessionCacheKey_(sessionToken);
  if (!cacheKey) return;
  try {
    CacheService.getScriptCache().remove(cacheKey);
  } catch (e) {}
}

function getAuthenticatedSession_() {
  var apiSession = getApiSessionFromRequestContext_();
  if (apiSession && apiSession.username) return apiSession;
  try {
    var rawUserCache = CacheService.getUserCache().get('auth_session_current');
    if (rawUserCache) {
      var parsedUserCache = JSON.parse(rawUserCache);
      if (parsedUserCache && parsedUserCache.username) return parsedUserCache;
    }
  } catch (e) {}

  try {
    var cacheKey = getAuthSessionCacheKey_();
    if (cacheKey) {
      var rawScriptCache = CacheService.getScriptCache().get(cacheKey);
      if (rawScriptCache) {
        var parsedScriptCache = JSON.parse(rawScriptCache);
        if (parsedScriptCache && parsedScriptCache.username) return parsedScriptCache;
      }
    }
  } catch (e) {}

  return null;
}

function clearAuthenticatedSession_() {
  try {
    CacheService.getUserCache().remove('auth_session_current');
  } catch (e) {}

  try {
    var cacheKey = getAuthSessionCacheKey_();
    if (cacheKey) {
      CacheService.getScriptCache().remove(cacheKey);
    }
  } catch (e) {}

  try {
    if (ACTIVE_API_REQUEST_CONTEXT_ && ACTIVE_API_REQUEST_CONTEXT_.sessionToken) {
      clearApiSessionToken_(ACTIVE_API_REQUEST_CONTEXT_.sessionToken);
      ACTIVE_API_REQUEST_CONTEXT_.sessionData = null;
    }
  } catch (e) {}
}

function touchAuthenticatedSession_(sessionData) {
  if (!sessionData || !sessionData.username) return null;

  var refreshedSession = {
    userId: String(sessionData.userId || '').trim(),
    username: String(sessionData.username || '').trim(),
    fullName: String(sessionData.fullName || sessionData.username || '').trim(),
    role: String(sessionData.role || 'staff').trim() || 'staff',
    issuedAt: String(sessionData.issuedAt || new Date().toISOString()),
    lastSeenAt: new Date().toISOString()
  };

  try {
    CacheService.getUserCache().put('auth_session_current', JSON.stringify(refreshedSession), AUTH_SESSION_TTL_SECONDS);
  } catch (e) {}

  try {
    var cacheKey = getAuthSessionCacheKey_();
    if (cacheKey) {
      CacheService.getScriptCache().put(cacheKey, JSON.stringify(refreshedSession), AUTH_SESSION_TTL_SECONDS);
    }
  } catch (e) {}

  try {
    if (ACTIVE_API_REQUEST_CONTEXT_ && ACTIVE_API_REQUEST_CONTEXT_.sessionToken) {
      touchApiSessionToken_(ACTIVE_API_REQUEST_CONTEXT_.sessionToken, refreshedSession);
    }
  } catch (e) {}

  return refreshedSession;
}

function requireAuthenticatedSession_() {
  var sessionData = getAuthenticatedSession_();
  if (!sessionData || !sessionData.username) {
    throw new Error('UNAUTHORIZED: กรุณาเข้าสู่ระบบใหม่อีกครั้ง');
  }

  var authSnapshot = getUserAuthorizationSnapshot_(sessionData.username);
  if (!authSnapshot || !authSnapshot.username) {
    clearAuthenticatedSession_();
    throw new Error('UNAUTHORIZED: ไม่พบบัญชีผู้ใช้งานในระบบ กรุณาเข้าสู่ระบบใหม่อีกครั้ง');
  }

  var normalizedStatus = normalizeUserStatus_(authSnapshot.status);
  if (normalizedStatus !== USER_STATUS_ACTIVE) {
    clearAuthenticatedSession_();
    throw new Error(
      normalizedStatus === USER_STATUS_PENDING
        ? 'FORBIDDEN: บัญชีของคุณอยู่ระหว่างรออนุมัติจากผู้ดูแลระบบ'
        : 'FORBIDDEN: บัญชีผู้ใช้นี้ถูกระงับการใช้งาน กรุณาติดต่อผู้ดูแลระบบ'
    );
  }

  if (!String(authSnapshot.emailVerifiedAt || '').trim()) {
    clearAuthenticatedSession_();
    throw new Error('FORBIDDEN: บัญชีนี้ยังไม่ได้ยืนยันอีเมล กรุณายืนยันอีเมลก่อนใช้งาน');
  }
  if (!authSnapshot.hasPin) {
    clearAuthenticatedSession_();
    throw new Error('FORBIDDEN: บัญชีนี้ยังไม่ได้ตั้ง PIN กรุณาตั้ง PIN ก่อนใช้งาน');
  }

  return touchAuthenticatedSession_({
    userId: authSnapshot.userId || sessionData.userId,
    username: authSnapshot.username || sessionData.username,
    fullName: authSnapshot.fullName || sessionData.fullName || sessionData.username,
    role: authSnapshot.role || sessionData.role,
    issuedAt: sessionData.issuedAt || new Date().toISOString()
  }) || sessionData;
}

function getCurrentActorName_() {
  var sessionData = getAuthenticatedSession_();
  if (!sessionData) return 'System Admin';
  return sessionData.fullName || sessionData.username || 'System Admin';
}

function getLoginLockoutState_(username) {
  var cache = CacheService.getScriptCache();
  var lockValue = cache.get(getLoginLockCacheKey_(username));
  return lockValue ? { locked: true } : { locked: false };
}

function registerFailedLoginAttempt_(username) {
  var cache = CacheService.getScriptCache();
  var attemptKey = getLoginAttemptCacheKey_(username);
  var currentAttempts = Number(cache.get(attemptKey) || 0) + 1;
  if (currentAttempts >= MAX_LOGIN_ATTEMPTS) {
    cache.put(getLoginLockCacheKey_(username), '1', LOGIN_LOCKOUT_SECONDS);
    cache.remove(attemptKey);
    return { locked: true, attempts: currentAttempts };
  }
  cache.put(attemptKey, String(currentAttempts), LOGIN_LOCKOUT_SECONDS);
  return { locked: false, attempts: currentAttempts };
}

function clearFailedLoginAttempts_(username) {
  var cache = CacheService.getScriptCache();
  cache.remove(getLoginAttemptCacheKey_(username));
  cache.remove(getLoginLockCacheKey_(username));
}

function logoutCurrentSession() {
  clearAuthenticatedSession_();
  return { status: 'Success', clearSessionToken: true };
}

function getSessionSnapshot() {
  var session = requireAuthenticatedSession_();
  return {
    status: 'Success',
    username: String(session.username || '').trim(),
    fullName: String(session.fullName || session.username || '').trim(),
    role: String(session.role || 'staff').trim() || 'staff',
    serverTime: getThaiDate(new Date()).dateTime
  };
}


var USERS_SHEET_NAME = 'Users';
var USERS_SHEET_HEADERS = [
  'UserID',
  'Username',
  'FullName',
  'Email',
  'PasswordHash',
  'Role',
  'Status',
  'CreatedAt',
  'UpdatedAt',
  'LastLoginAt',
  'ResetOtpHash',
  'ResetOtpExpireAt',
  'ResetOtpRequestedAt',
  'ResetOtpUsedAt',
  'PermissionsJson',
  'Prefix',
  'FirstName',
  'LastName',
  'EmailVerifiedAt',
  'PinHash',
  'PinUniqueKey',
  'PinUpdatedAt',
  'EmailVerifyOtpHash',
  'EmailVerifyOtpExpireAt',
  'EmailVerifyOtpRequestedAt'
];
var USER_STATUS_PENDING = 'Pending';
var USER_STATUS_ACTIVE = 'Active';
var USER_STATUS_SUSPENDED = 'Suspended';
var PASSWORD_RESET_OTP_TTL_MINUTES = 15;
var EMAIL_VERIFICATION_OTP_TTL_MINUTES = 15;
var AUDIT_LOGS_SHEET_NAME = 'AuditLogs';
var AUDIT_LOGS_HEADERS = [
  'วัน/เดือน/ปี (พ.ศ.)',
  'Username',
  'ชื่อผู้ใช้',
  'Role',
  'Action',
  'EntityType',
  'EntityId',
  'ReferenceNo',
  'BeforeJson',
  'AfterJson',
  'Reason',
  'Details'
];
var COUNTERS_SHEET_NAME = 'Counters';
var COUNTERS_HEADERS = ['CounterKey', 'CurrentNo', 'UpdatedAt'];

function ensureUsersSheet_(ss) {
  ss = ss || getDatabaseSpreadsheet_();
  var sheet = ensureSheetWithHeadersSafely_(
    ss,
    USERS_SHEET_NAME,
    USERS_SHEET_HEADERS,
    '#dcfce7',
    'เตรียมชีต Users'
  );
  setPlainTextColumnFormat_(sheet, [1, 2, 4, 5, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25]);

  if (sheet.getFrozenRows() < 1) {
    sheet.setFrozenRows(1);
  }

  migrateLegacyAdminUserToUsersSheet_(ss, sheet);
  return sheet;
}

function migrateLegacyAdminUserToUsersSheet_(ss, usersSheet) {
  if (!usersSheet) return null;

  var existingAdmin = findUserByUsernameInternal_(usersSheet, 'admin');
  if (existingAdmin && existingAdmin.rowNumber > 1) {
    return existingAdmin;
  }

  var settingsSheet = ss.getSheetByName('Settings') || createSheetSafely_(ss, 'Settings', 3, 2, 'เตรียมชีต Settings');
  var settingsData = settingsSheet.getLastRow() > 0
    ? settingsSheet.getRange(1, 1, settingsSheet.getLastRow(), Math.max(2, settingsSheet.getLastColumn())).getValues()
    : [];
  var adminHash = '';
  var hasInterestRate = false;

  for (var i = 0; i < settingsData.length; i++) {
    var settingKey = String(settingsData[i][0] || '').trim();
    if (settingKey === 'AdminPasswordHash') {
      adminHash = String(settingsData[i][1] || '').trim();
    }
    if (settingKey === 'InterestRate') {
      hasInterestRate = true;
    }
  }

  if (!adminHash) {
    var initialAdminPassword = String(PropertiesService.getScriptProperties().getProperty('INITIAL_ADMIN_PASSWORD') || '').trim();
    if (!initialAdminPassword) {
      throw new Error('กรุณาตั้งค่า Script Property ชื่อ INITIAL_ADMIN_PASSWORD ก่อนใช้งานครั้งแรก');
    }

    adminHash = createPasswordHashRecord_(initialAdminPassword);

    if (settingsSheet.getLastRow() === 0) {
      settingsSheet.getRange(1, 1, 1, 2).setValues([
        ['หัวข้อการตั้งค่า', 'ค่าการตั้งค่า']
      ]).setFontWeight('bold');
    }

    if (!hasInterestRate) {
      appendRowsSafely_(settingsSheet, [['InterestRate', 12.0]], 'เพิ่มค่า InterestRate ใน Settings');
    }
    appendRowsSafely_(settingsSheet, [['AdminPasswordHash', adminHash]], 'เพิ่มค่า AdminPasswordHash ใน Settings');
  }

  var nowText = getThaiDate(new Date()).dateTime;
  var defaultAdminPin = String(PropertiesService.getScriptProperties().getProperty('INITIAL_ADMIN_PIN') || '123456').trim();
  if (!/^\d{6}$/.test(defaultAdminPin)) defaultAdminPin = '123456';
  var adminRow = [
    Utilities.getUuid(),
    'admin',
    'ผู้ดูแลระบบ',
    'admin@local.invalid',
    adminHash,
    'admin',
    USER_STATUS_ACTIVE,
    nowText,
    nowText,
    '',
    '',
    '',
    '',
    '',
    '',
    '',
    '',
    nowText,
    createPasswordHashRecord_(defaultAdminPin),
    computePinUniqueKey_(defaultAdminPin),
    nowText,
    '',
    '',
    ''
  ];
  var insertedAdminRow = appendRowsSafely_(usersSheet, [adminRow], 'เพิ่มผู้ใช้ admin เริ่มต้น');
  return buildUserRecordFromSheetRow_(usersSheet, insertedAdminRow, adminRow);
}

function buildUserRecordFromSheetRow_(sheet, rowNumber, rowValues) {
  var row = rowValues || [];
  return {
    rowNumber: rowNumber,
    userId: String(row[0] || '').trim(),
    username: String(row[1] || '').trim().toLowerCase(),
    fullName: String(row[2] || '').trim(),
    email: String(row[3] || '').trim().toLowerCase(),
    passwordHash: String(row[4] || '').trim(),
    role: String(row[5] || '').trim().toLowerCase() || 'staff',
    status: String(row[6] || '').trim() || USER_STATUS_ACTIVE,
    createdAt: String(row[7] || '').trim(),
    updatedAt: String(row[8] || '').trim(),
    lastLoginAt: String(row[9] || '').trim(),
    resetOtpHash: String(row[10] || '').trim(),
    resetOtpExpireAt: String(row[11] || '').trim(),
    resetOtpRequestedAt: String(row[12] || '').trim(),
    resetOtpUsedAt: String(row[13] || '').trim(),
    permissionsJson: String(row[14] || '').trim(),
    prefix: String(row[15] || '').trim(),
    firstName: String(row[16] || '').trim(),
    lastName: String(row[17] || '').trim(),
    emailVerifiedAt: String(row[18] || '').trim(),
    pinHash: String(row[19] || '').trim(),
    pinUniqueKey: String(row[20] || '').trim(),
    pinUpdatedAt: String(row[21] || '').trim(),
    emailVerifyOtpHash: String(row[22] || '').trim(),
    emailVerifyOtpExpireAt: String(row[23] || '').trim(),
    emailVerifyOtpRequestedAt: String(row[24] || '').trim()
  };
}

function getAllUserRecords_(usersSheet) {
  if (!usersSheet || usersSheet.getLastRow() <= 1) return [];
  var values = usersSheet.getRange(2, 1, usersSheet.getLastRow() - 1, USERS_SHEET_HEADERS.length).getValues();
  var records = [];
  for (var i = 0; i < values.length; i++) {
    records.push(buildUserRecordFromSheetRow_(usersSheet, i + 2, values[i]));
  }
  return records;
}

function normalizeUsername_(value) {
  return String(value || '').replace(/\s+/g, '').trim().toLowerCase();
}

function normalizeEmail_(value) {
  return String(value || '').trim().toLowerCase();
}

function normalizePersonPrefix_(value) {
  return String(value || '').replace(/\s+/g, ' ').trim();
}

function normalizePersonNamePart_(value) {
  return String(value || '').replace(/\s+/g, ' ').trim();
}

function buildFullNameFromParts_(prefix, firstName, lastName) {
  return [
    normalizePersonPrefix_(prefix),
    normalizePersonNamePart_(firstName),
    normalizePersonNamePart_(lastName)
  ].filter(function(item) { return !!item; }).join(' ').replace(/\s+/g, ' ').trim();
}

function normalizePinInput_(value) {
  return String(value || '').replace(/\D/g, '').trim();
}

function isValidSixDigitPin_(value) {
  return /^\d{6}$/.test(String(value || '').trim());
}

function computePinUniqueKey_(pinValue) {
  var pin = normalizePinInput_(pinValue);
  if (!pin) return '';
  var pepper = String(PropertiesService.getScriptProperties().getProperty('PIN_UNIQUE_PEPPER') || 'PIN_UNIQUE_PEPPER_DEFAULT').trim();
  var digest = Utilities.computeDigest(Utilities.DigestAlgorithm.SHA_256, pin + '|' + pepper);
  var out = '';
  for (var i = 0; i < digest.length; i++) {
    var hex = (digest[i] & 0xFF).toString(16);
    if (hex.length < 2) hex = '0' + hex;
    out += hex;
  }
  return out;
}

function findUserByPinUniqueKeyInternal_(usersSheet, pinUniqueKey) {
  var normalized = String(pinUniqueKey || '').trim();
  if (!normalized) return null;
  var users = getAllUserRecords_(usersSheet);
  for (var i = 0; i < users.length; i++) {
    if (String(users[i].pinUniqueKey || '').trim() === normalized) return users[i];
  }
  return null;
}

function ensurePinUniqueAcrossUsers_(usersSheet, pinValue, excludeUserId) {
  var pinUniqueKey = computePinUniqueKey_(pinValue);
  if (!pinUniqueKey) throw new Error('PIN ไม่ถูกต้อง');
  var owner = findUserByPinUniqueKeyInternal_(usersSheet, pinUniqueKey);
  if (!owner) return pinUniqueKey;
  var excluded = String(excludeUserId || '').trim();
  if (excluded && String(owner.userId || '').trim() === excluded) return pinUniqueKey;
  throw new Error('PIN นี้ถูกใช้งานแล้ว กรุณาเลือก PIN ใหม่');
}

function isValidEmail_(email) {
  return /^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(String(email || '').trim());
}

function maskEmailForDisplay_(email) {
  var normalized = normalizeEmail_(email);
  if (!normalized || normalized.indexOf('@') === -1) return '';
  var parts = normalized.split('@');
  var local = parts[0];
  var domain = parts[1];
  if (local.length <= 2) return local.charAt(0) + '*@' + domain;
  return local.substring(0, 2) + '***@' + domain;
}

function findUserByUsernameInternal_(usersSheet, username) {
  var normalized = normalizeUsername_(username);
  if (!normalized) return null;
  var users = getAllUserRecords_(usersSheet);
  for (var i = 0; i < users.length; i++) {
    if (users[i].username === normalized) return users[i];
  }
  return null;
}

function findUserByIdentifierInternal_(usersSheet, identifier) {
  var normalized = String(identifier || '').trim();
  var normalizedUsername = normalizeUsername_(normalized);
  var normalizedEmail = normalizeEmail_(normalized);
  if (!normalizedUsername && !normalizedEmail) return null;
  var users = getAllUserRecords_(usersSheet);
  for (var i = 0; i < users.length; i++) {
    if (users[i].username === normalizedUsername || (users[i].email && users[i].email === normalizedEmail)) {
      return users[i];
    }
  }
  return null;
}

function updateUserCellsByRecord_(usersSheet, userRecord, patch) {
  if (!usersSheet || !userRecord || !userRecord.rowNumber || !patch) return;

  var fieldToColumn = {
    userId: 1,
    username: 2,
    fullName: 3,
    email: 4,
    passwordHash: 5,
    role: 6,
    status: 7,
    createdAt: 8,
    updatedAt: 9,
    lastLoginAt: 10,
    resetOtpHash: 11,
    resetOtpExpireAt: 12,
    resetOtpRequestedAt: 13,
    resetOtpUsedAt: 14,
    permissionsJson: 15,
    prefix: 16,
    firstName: 17,
    lastName: 18,
    emailVerifiedAt: 19,
    pinHash: 20,
    pinUniqueKey: 21,
    pinUpdatedAt: 22,
    emailVerifyOtpHash: 23,
    emailVerifyOtpExpireAt: 24,
    emailVerifyOtpRequestedAt: 25
  };

  var keys = Object.keys(patch);
  for (var i = 0; i < keys.length; i++) {
    var key = keys[i];
    var col = fieldToColumn[key];
    if (!col) continue;
    usersSheet.getRange(userRecord.rowNumber, col).setValue(patch[key]);
    userRecord[key] = patch[key];
  }
}

function generateSixDigitOtp_() {
  var seed = [
    Utilities.getUuid(),
    String(new Date().getTime()),
    String(Session.getTemporaryActiveUserKey() || '')
  ].join('|');

  var digest = Utilities.computeDigest(Utilities.DigestAlgorithm.SHA_256, seed);
  var digits = '';

  for (var i = 0; i < digest.length; i++) {
    digits += String(Math.abs(digest[i]) % 10);
    if (digits.length >= 6) break;
  }

  while (digits.length < 6) digits += '0';
  return digits.substring(0, 6);
}

function addMinutesToDate_(dateObj, minutes) {
  return new Date(dateObj.getTime() + (Number(minutes) || 0) * 60000);
}

function buildPasswordResetEmailBody_(displayName, otpCode) {
  var safeName = String(displayName || 'ผู้ใช้งาน').trim() || 'ผู้ใช้งาน';
  var safeOtp = String(otpCode || '').trim();
  return [
    'เรียน ' + safeName,
    '',
    'รหัส OTP สำหรับตั้ง PIN ใหม่ของระบบจัดการเงินกู้คือ: ' + safeOtp,
    'รหัสนี้มีอายุ ' + PASSWORD_RESET_OTP_TTL_MINUTES + ' นาที',
    '',
    'หากคุณไม่ได้เป็นผู้ร้องขอ โปรดละเว้นอีเมลฉบับนี้',
    '',
    'ระบบจัดการเงินกู้ บ้านพิตำ'
  ].join('\n');
}

function normalizeUserRole_(value) {
  var role = String(value || '').trim().toLowerCase();
  if (!role) return 'staff';
  if (role === 'admin') return 'admin';
  return role.replace(/[^a-z0-9._-]/g, '');
}

function normalizeUserStatus_(value) {
  var status = String(value || '').trim();
  if (!status) return USER_STATUS_PENDING;
  var lower = status.toLowerCase();
  if (lower === 'active') return USER_STATUS_ACTIVE;
  if (lower === 'suspended' || lower === 'banned' || lower === 'disabled' || lower === 'inactive') return USER_STATUS_SUSPENDED;
  if (lower === 'pending' || lower === 'waiting' || lower === 'awaiting_approval') return USER_STATUS_PENDING;
  return status;
}

var PERMISSION_CATALOG = [
  { key: 'dashboard.view', label: 'ดูภาพรวมระบบ' },
  { key: 'payments.create', label: 'บันทึกรับชำระเงิน' },
  { key: 'payments.edit', label: 'แก้ไขรายการรับชำระ' },
  { key: 'payments.reverse', label: 'กลับรายการรับชำระ' },
  { key: 'payment_records.view', label: 'ดูเมนูจัดการรายการรับชำระ' },
  { key: 'qualifications.view', label: 'ดูเมนูตรวจสอบคุณสมบัติ' },
  { key: 'members.view', label: 'ดูทะเบียนสมาชิก' },
  { key: 'members.manage', label: 'เพิ่ม/แก้ไข/ลบสมาชิก' },
  { key: 'loans.view', label: 'ดูข้อมูลสัญญาเงินกู้' },
  { key: 'loans.manage', label: 'เพิ่ม/แก้ไข/ลบสัญญา' },
  { key: 'reports.view', label: 'ดูรายงาน' },
  { key: 'reports.archive', label: 'บันทึก/เปิดคลังรายงาน PDF' },
  { key: 'reports.export', label: 'ส่งออกข้อมูล CSV' },
  { key: 'reports.reconcile', label: 'ตรวจความสอดคล้องของยอด' },
  { key: 'profile.view', label: 'ดูข้อมูลส่วนตัว' },
  { key: 'settings.view', label: 'ดูเมนูตั้งค่าระบบ' },
  { key: 'settings.manage', label: 'แก้ไขการตั้งค่าระบบ' },
  { key: 'users.manage', label: 'จัดการผู้ใช้งาน' },
  { key: 'audit.view', label: 'ดู Audit Logs' },
  { key: 'accounting.close', label: 'ปิดยอดบัญชี' },
  { key: 'backup.run', label: 'Backup ธุรกรรม' }
];

function getPermissionCatalog_() {
  return PERMISSION_CATALOG.map(function(item) {
    return { key: String(item.key || ''), label: String(item.label || item.key || '') };
  });
}

function getAllPermissionKeys_() {
  return PERMISSION_CATALOG.map(function(item) { return String(item.key || '').trim(); }).filter(function(key) { return !!key; });
}

function getDefaultPermissionMapByRole_(role) {
  var normalizedRole = normalizeUserRole_(role);
  var map = {};
  var keys = getAllPermissionKeys_();
  for (var i = 0; i < keys.length; i++) map[keys[i]] = false;

  if (normalizedRole === 'admin') {
    for (var j = 0; j < keys.length; j++) map[keys[j]] = true;
    return map;
  }

  var staffDefaults = [
    'dashboard.view',
    'payments.create',
    'payment_records.view',
    'qualifications.view',
    'members.view',
    'loans.view',
    'reports.view',
    'reports.export',
    'profile.view'
  ];
  for (var s = 0; s < staffDefaults.length; s++) {
    map[staffDefaults[s]] = true;
  }
  return map;
}

function parsePermissionsJson_(value) {
  if (!value && value !== false) return {};
  if (Array.isArray(value)) {
    var arrMap = {};
    for (var i = 0; i < value.length; i++) {
      var key = String(value[i] || '').trim();
      if (key) arrMap[key] = true;
    }
    return arrMap;
  }
  if (typeof value === 'object') {
    var objMap = {};
    for (var objKey in value) {
      if (!Object.prototype.hasOwnProperty.call(value, objKey)) continue;
      objMap[String(objKey || '').trim()] = !!value[objKey];
    }
    return objMap;
  }
  var text = String(value || '').trim();
  if (!text) return {};
  try {
    var parsed = JSON.parse(text);
    return parsePermissionsJson_(parsed);
  } catch (e) {
    return {};
  }
}

function normalizePermissionsJson_(value) {
  var parsed = parsePermissionsJson_(value);
  var keys = getAllPermissionKeys_();
  var normalized = {};
  for (var i = 0; i < keys.length; i++) {
    var key = keys[i];
    if (parsed[key] === true) normalized[key] = true;
    else if (parsed[key] === false) normalized[key] = false;
  }
  return JSON.stringify(normalized);
}

function getEffectivePermissionMapForUserRecord_(userRecord) {
  var baseMap = getDefaultPermissionMapByRole_(userRecord && userRecord.role);
  if (normalizeUserRole_(userRecord && userRecord.role) === 'admin') {
    return baseMap;
  }
  var customMap = parsePermissionsJson_(userRecord && userRecord.permissionsJson);
  var keys = getAllPermissionKeys_();
  for (var i = 0; i < keys.length; i++) {
    var key = keys[i];
    if (customMap[key] === true || customMap[key] === false) {
      baseMap[key] = customMap[key];
    }
  }
  return baseMap;
}

function getEffectivePermissionsForUserRecord_(userRecord) {
  var map = getEffectivePermissionMapForUserRecord_(userRecord);
  var keys = Object.keys(map);
  var list = [];
  for (var i = 0; i < keys.length; i++) {
    if (map[keys[i]]) list.push(keys[i]);
  }
  list.sort();
  return list;
}

function hasPermissionFromSnapshot_(authSnapshot, permissionKey) {
  var normalizedKey = String(permissionKey || '').trim();
  if (!normalizedKey) return false;
  var map = parsePermissionsJson_(authSnapshot && authSnapshot.permissionsMap);
  if (normalizeUserRole_(authSnapshot && authSnapshot.role) === 'admin') return true;
  if (map && Object.prototype.hasOwnProperty.call(map, normalizedKey)) {
    return !!map[normalizedKey];
  }
  return false;
}

function getCurrentUserPermissionSet_() {
  var sessionData = requireAuthenticatedSession_();
  var authSnapshot = getUserAuthorizationSnapshot_(sessionData.username);
  var map = parsePermissionsJson_(authSnapshot && authSnapshot.permissionsMap);
  var keys = Object.keys(map);
  var list = [];
  for (var i = 0; i < keys.length; i++) {
    if (map[keys[i]]) list.push(keys[i]);
  }
  list.sort();
  return list;
}

function requirePermission_(permissionKey, friendlyMessage) {
  var sessionData = requireAuthenticatedSession_();
  var authSnapshot = getUserAuthorizationSnapshot_(sessionData.username);
  if (normalizeUserRole_(authSnapshot && authSnapshot.role) === 'admin') return sessionData;
  if (!hasPermissionFromSnapshot_(authSnapshot, permissionKey)) {
    throw new Error('FORBIDDEN: ' + (friendlyMessage || ('ไม่มีสิทธิ์ใช้งานฟังก์ชัน ' + permissionKey)));
  }
  return sessionData;
}

function requireAdminSession_() {
  var sessionData = requireAuthenticatedSession_();
  if (normalizeUserRole_(sessionData.role) !== 'admin') {
    throw new Error('FORBIDDEN: เฉพาะผู้ดูแลระบบเท่านั้น');
  }
  return sessionData;
}

function sanitizeUserRecordForClient_(userRecord) {
  if (!userRecord) return null;
  return {
    userId: String(userRecord.userId || '').trim(),
    username: String(userRecord.username || '').trim(),
    fullName: String(userRecord.fullName || '').trim(),
    email: String(userRecord.email || '').trim(),
    prefix: String(userRecord.prefix || '').trim(),
    firstName: String(userRecord.firstName || '').trim(),
    lastName: String(userRecord.lastName || '').trim(),
    emailVerifiedAt: String(userRecord.emailVerifiedAt || '').trim(),
    role: normalizeUserRole_(userRecord.role),
    status: normalizeUserStatus_(userRecord.status),
    createdAt: String(userRecord.createdAt || '').trim(),
    updatedAt: String(userRecord.updatedAt || '').trim(),
    lastLoginAt: String(userRecord.lastLoginAt || '').trim(),
    permissionsJson: String(userRecord.permissionsJson || '').trim(),
    permissions: getEffectivePermissionsForUserRecord_(userRecord)
  };
}

function countActiveAdmins_(usersSheet, excludeUserId) {
  var users = getAllUserRecords_(usersSheet);
  var count = 0;
  var excluded = String(excludeUserId || '').trim();
  for (var i = 0; i < users.length; i++) {
    var user = users[i];
    if (excluded && String(user.userId || '').trim() === excluded) continue;
    if (normalizeUserRole_(user.role) === 'admin' && normalizeUserStatus_(user.status) === USER_STATUS_ACTIVE) {
      count++;
    }
  }
  return count;
}

function findUserByIdInternal_(usersSheet, userId) {
  var normalized = String(userId || '').trim();
  if (!normalized) return null;
  var users = getAllUserRecords_(usersSheet);
  for (var i = 0; i < users.length; i++) {
    if (String(users[i].userId || '').trim() === normalized) return users[i];
  }
  return null;
}

function ensureAuditLogsSheet_(ss) {
  var spreadsheet = ss || getDatabaseSpreadsheet_();
  var sheet = ensureSheetWithHeadersSafely_(spreadsheet, AUDIT_LOGS_SHEET_NAME, AUDIT_LOGS_HEADERS, '#e0f2fe', 'เตรียมชีต AuditLogs');
  setPlainTextColumnFormat_(sheet, [1, 2, 3, 4, 5, 6, 7, 8]);
  if (sheet.getFrozenRows() < 1) sheet.setFrozenRows(1);
  return sheet;
}

function ensureCountersSheet_(ss) {
  var spreadsheet = ss || getDatabaseSpreadsheet_();
  var sheet = ensureSheetWithHeadersSafely_(spreadsheet, COUNTERS_SHEET_NAME, COUNTERS_HEADERS, '#fef3c7', 'เตรียมชีต Counters');
  setPlainTextColumnFormat_(sheet, [1, 3]);
  if (sheet.getFrozenRows() < 1) sheet.setFrozenRows(1);
  return sheet;
}

function getCurrentActorUsername_() {
  var sessionData = getAuthenticatedSession_();
  if (!sessionData) return 'system';
  return String(sessionData.username || '').trim() || 'system';
}

function serializeAuditValue_(value) {
  if (value === null || value === undefined || value === '') return '';
  if (typeof value === 'string') return value;
  try {
    return JSON.stringify(value);
  } catch (e) {
    return String(value);
  }
}

function buildAuditTransactionSnapshotFromValues_(rowValues) {
  var safeValues = (rowValues || []).slice();
  while (safeValues.length < 14) safeValues.push('');
  return {
    id: String(safeValues[0] || '').trim(),
    timestamp: normalizeThaiDateCellValue_(safeValues[1], true),
    contract: String(safeValues[2] || '').trim(),
    memberId: String(safeValues[3] || '').trim(),
    principalPaid: Number(safeValues[4]) || 0,
    interestPaid: Number(safeValues[5]) || 0,
    newBalance: Number(safeValues[6]) || 0,
    note: String(safeValues[7] || '').trim(),
    actorName: String(safeValues[8] || '').trim(),
    memberName: String(safeValues[9] || '').trim(),
    txStatus: String(safeValues[10] || '').trim() || 'ปกติ',
    interestMonthsPaid: Number(safeValues[11]) || 0,
    missedBeforePayment: Number(safeValues[12]) || 0,
    missedAfterPayment: Number(safeValues[13]) || 0
  };
}


function normalizeTransactionStatus_(status) {
  return String(status || '').trim() || 'ปกติ';
}

function isInactiveTransactionStatus_(status) {
  var normalizedStatus = normalizeTransactionStatus_(status);
  return normalizedStatus === 'ยกเลิก' || normalizedStatus === 'กลับรายการ';
}

function isReversalTransactionStatus_(status) {
  return normalizeTransactionStatus_(status) === 'กลับรายการ';
}

function sanitizeReverseReason_(reason) {
  return String(reason || '').replace(/\s+/g, ' ').trim().substring(0, 180);
}

function buildReverseTransactionNote_(sourceTxId, reason, reversalTxId) {
  var parts = [];
  parts.push('กลับรายการอ้างอิง ' + String(sourceTxId || '-').trim());
  if (reversalTxId) parts.push('เลขที่กลับรายการ ' + String(reversalTxId || '').trim());
  if (reason) parts.push('เหตุผล: ' + String(reason || '').trim());
  return parts.join(' | ').substring(0, 500);
}

function updateTransactionRowStatus_(matchedTx, txStatus, noteText) {
  if (!matchedTx || !matchedTx.sheet || !matchedTx.rowIndex || matchedTx.rowIndex <= 1) return;
  var nextStatus = normalizeTransactionStatus_(txStatus);
  var nextNote = String(noteText || '').trim();
  matchedTx.sheet.getRange(matchedTx.rowIndex, 8).setValue(nextNote);
  matchedTx.sheet.getRange(matchedTx.rowIndex, 11).setValue(nextStatus);
  if (matchedTx.values && matchedTx.values.length) {
    matchedTx.values[7] = nextNote;
    matchedTx.values[10] = nextStatus;
  }
  invalidateSheetExecutionCaches_(matchedTx.sheet);
}

function appendReversalTransactionRecord_(ss, matchedTx, reason, options) {
  var spreadsheet = ss || getDatabaseSpreadsheet_();
  var sourceValues = (matchedTx && matchedTx.values ? matchedTx.values : []).slice();
  while (sourceValues.length < 16) sourceValues.push('');

  var snapshot = buildAuditTransactionSnapshotFromValues_(sourceValues);
  var reversalTxId = generateDocumentReference_('REV', new Date(), spreadsheet);
  var reversalTimestamp = getThaiDate(new Date()).dateTime;
  var resultingBalance = options && options.finalLoanState ? Number(options.finalLoanState.balance) || 0 : snapshot.newBalance;
  var resultingMissed = options && options.finalLoanState ? Math.max(0, Number(options.finalLoanState.missedInterestMonths) || 0) : snapshot.missedAfterPayment;
  var reversalNote = buildReverseTransactionNote_(snapshot.id, reason, reversalTxId);
  var actorName = getCurrentActorName_();

  var reversalRow = [
    reversalTxId,
    reversalTimestamp,
    snapshot.contract,
    snapshot.memberId,
    snapshot.principalPaid,
    snapshot.interestPaid,
    resultingBalance,
    reversalNote,
    actorName,
    snapshot.memberName,
    'กลับรายการ',
    snapshot.interestMonthsPaid,
    snapshot.missedAfterPayment,
    resultingMissed,
    sourceValues[14] || '',
    sourceValues[15] || ''
  ];

  var targetSheet = matchedTx && matchedTx.sheet ? matchedTx.sheet : resolveWritableTransactionsSheet_(spreadsheet);
  ensureTransactionsSheetSchema_(targetSheet);
  var reversalRowIndex = appendRowsSafely_(targetSheet, [reversalRow], 'บันทึกรายการกลับรายการ');
  upsertTransactionIndexEntry_(spreadsheet, targetSheet, reversalRowIndex, reversalRow);

  return {
    txId: reversalTxId,
    timestamp: reversalTimestamp,
    note: reversalNote,
    rowValues: reversalRow,
    rowIndex: reversalRowIndex,
    sheetName: String(targetSheet.getName() || '')
  };
}

function reverseManagedPaymentByMatchedTransaction_(ss, matchedTx, reason, actionLabel, successMessage) {
  if (!ss || !matchedTx) {
    throw new Error('ไม่พบข้อมูลรายการรับชำระที่ต้องการกลับรายการ');
  }

  var normalizedReason = sanitizeReverseReason_(reason);
  if (!normalizedReason) {
    throw new Error('กรุณาระบุเหตุผลการกลับรายการ');
  }

  var txId = String((matchedTx.values || [])[0] || '').trim();
  var contractNo = String((matchedTx.values || [])[2] || '').trim();
  var txStatus = String((matchedTx.values || [])[10] || '').trim();
  if (!txId) throw new Error('ไม่พบรหัสอ้างอิงของรายการรับชำระ');
  if (!contractNo) throw new Error('ไม่พบเลขที่สัญญาของรายการรับชำระ');
  if (isInactiveTransactionStatus_(txStatus)) {
    throw new Error(isReversalTransactionStatus_(txStatus)
      ? 'รายการนี้เป็นรายการกลับรายการอยู่แล้ว'
      : 'รายการนี้ถูกยกเลิก/กลับรายการแล้ว');
  }

  var originalSnapshot = buildAuditTransactionSnapshotFromValues_(matchedTx.values);
  var plan = buildManagedContractRecalculationPlan_(ss, contractNo, {
    targetTxId: txId,
    targetMode: 'remove',
    replacementMatch: matchedTx
  });

  persistManagedContractRecalculationPlan_(ss, plan, []);
  var reversalMeta = appendReversalTransactionRecord_(ss, matchedTx, normalizedReason, {
    finalLoanState: plan.finalLoanState
  });
  updateTransactionRowStatus_(matchedTx, 'ยกเลิก', buildReverseTransactionNote_(txId, normalizedReason, reversalMeta.txId));
  var refreshedSourceTx = findExistingTransactionById_(ss, txId);
  syncTransactionIndexEntries_(ss, [{
    txId: txId,
    rowValues: refreshedSourceTx ? refreshedSourceTx.values : null,
    rowIndex: refreshedSourceTx ? refreshedSourceTx.rowIndex : 0,
    sourceSheet: refreshedSourceTx ? refreshedSourceTx.sheet : null
  }, {
    txId: reversalMeta.txId,
    rowValues: reversalMeta.rowValues,
    rowIndex: reversalMeta.rowIndex,
    sourceSheet: matchedTx && matchedTx.sheet ? matchedTx.sheet : null,
    sourceSheetName: reversalMeta.sheetName || ''
  }]);

  appendAuditLogSafe_(ss, {
    action: 'REVERSE_PAYMENT',
    entityType: 'TRANSACTION',
    entityId: txId,
    referenceNo: reversalMeta.txId,
    before: originalSnapshot,
    after: {
      reversalTransactionId: reversalMeta.txId,
      contract: contractNo,
      finalLoanState: plan.finalLoanState,
      sourceTransactionStatus: 'ยกเลิก',
      reversalTransactionStatus: 'กลับรายการ'
    },
    reason: normalizedReason,
    details: String(actionLabel || 'กลับรายการรับชำระย้อนหลัง') + ' สำเร็จ'
  });
  logSystemActionFast(
    ss,
    actionLabel || 'กลับรายการรับชำระย้อนหลัง',
    'Transaction ID: ' + txId + ' | Reversal ID: ' + reversalMeta.txId + ' | Contract: ' + contractNo + ' | Sheet: ' + String(matchedTx.sheet && matchedTx.sheet.getName ? matchedTx.sheet.getName() || '' : '')
  );
  syncPaymentSearchIndexForContracts_(ss, [contractNo]);
  syncPaymentTodayIndexEntries_(ss, [{
    txId: txId
  }, {
    txId: reversalMeta.txId,
    rowValues: reversalMeta.rowValues,
    rowIndex: reversalMeta.rowIndex
  }]);
  clearAppCache_();
  syncTransactionIndexVersionToCache_();
  syncPaymentSearchIndexVersionToCache_();
  syncPaymentTodayIndexVersionToCache_();

  return {
    status: 'Success',
    message: successMessage || 'กลับรายการรับชำระเรียบร้อยแล้ว',
    transactionId: txId,
    reversalTransactionId: reversalMeta.txId,
    contractNo: contractNo,
    finalLoanState: plan.finalLoanState
  };
}

function buildAuditLoanSnapshotFromRow_(rowValues) {
  var safeValues = (rowValues || []).slice();
  while (safeValues.length < 12) safeValues.push('');
  return {
    memberId: String(safeValues[0] || '').trim(),
    contract: String(safeValues[1] || '').trim(),
    memberName: String(safeValues[2] || '').trim(),
    amount: Number(safeValues[3]) || 0,
    interestRate: Number(safeValues[4]) || 0,
    balance: Number(safeValues[5]) || 0,
    status: String(safeValues[6] || '').trim(),
    nextPayment: String(safeValues[7] || '').trim(),
    missedInterestMonths: Number(safeValues[8]) || 0,
    createdAt: String(safeValues[9] || '').trim(),
    guarantor1: String(safeValues[10] || '').trim(),
    guarantor2: String(safeValues[11] || '').trim()
  };
}

function appendAuditLog_(ss, payload) {
  var spreadsheet = ss || getDatabaseSpreadsheet_();
  var safePayload = payload || {};
  var sheet = ensureAuditLogsSheet_(spreadsheet);
  var sessionData = getAuthenticatedSession_() || {};
  var createdAt = getThaiDate(new Date()).dateTime;
  appendRowsSafely_(sheet, [[
    createdAt,
    String(safePayload.username || sessionData.username || '').trim(),
    String(safePayload.actorName || sessionData.fullName || sessionData.username || '').trim(),
    String(safePayload.role || sessionData.role || '').trim(),
    String(safePayload.action || '').trim(),
    String(safePayload.entityType || '').trim(),
    String(safePayload.entityId || '').trim(),
    String(safePayload.referenceNo || '').trim(),
    serializeAuditValue_(safePayload.before),
    serializeAuditValue_(safePayload.after),
    String(safePayload.reason || '').trim(),
    String(safePayload.details || '').trim()
  ]], 'บันทึก Audit Log');
}

function appendAuditLogSafe_(ss, payload) {
  try {
    appendAuditLog_(ss, payload);
  } catch (auditError) {
    console.warn('appendAuditLogSafe_ failed', auditError);
  }
}

function getNextCounterValue_(ss, counterKey) {
  var spreadsheet = ss || getDatabaseSpreadsheet_();
  var sheet = ensureCountersSheet_(spreadsheet);
  var normalizedKey = String(counterKey || '').trim();
  if (!normalizedKey) throw new Error('ไม่พบ counter key');

  var rowIndex = findRowByExactValue(sheet, 1, normalizedKey);
  var nextValue = 1;
  var nowText = getThaiDate(new Date()).dateTime;

  if (rowIndex > 1) {
    nextValue = (Number(sheet.getRange(rowIndex, 2).getValue()) || 0) + 1;
    sheet.getRange(rowIndex, 2, 1, 2).setValues([[nextValue, nowText]]);
  } else {
    appendRowsSafely_(sheet, [[normalizedKey, nextValue, nowText]], 'เพิ่มตัวนับเอกสาร');
  }

  return nextValue;
}

function generateDocumentReference_(prefix, dateObj, ss) {
  var normalizedPrefix = String(prefix || 'DOC').trim().toUpperCase() || 'DOC';
  var parsed = parseThaiDateParts_(dateObj || new Date()) || {
    month: (dateObj || new Date()).getMonth() + 1,
    yearBe: (dateObj || new Date()).getFullYear() + 543
  };
  var yearBe = Number(parsed.yearBe) || ((dateObj || new Date()).getFullYear() + 543);
  var month = Number(parsed.month) || ((dateObj || new Date()).getMonth() + 1);
  var running = getNextCounterValue_(ss || getDatabaseSpreadsheet_(), normalizedPrefix + '|' + yearBe + padNumber_(month, 2));
  return normalizedPrefix + '-' + yearBe + padNumber_(month, 2) + '-' + padNumber_(running, 4);
}


function buildSimpleTransactionRecordFromValues_(rowValues, sourceSheetName, rowIndex) {
  var safeValues = (rowValues || []).slice();
  while (safeValues.length < 16) safeValues.push('');
  var timestamp = normalizeThaiDateCellValue_(safeValues[1], true);
  return {
    id: String(safeValues[0] || '').trim(),
    timestamp: timestamp,
    contract: String(safeValues[2] || '').trim(),
    memberId: String(safeValues[3] || '').trim(),
    principalPaid: Number(safeValues[4]) || 0,
    interestPaid: Number(safeValues[5]) || 0,
    newBalance: Number(safeValues[6]) || 0,
    note: String(safeValues[7] || '').trim(),
    actorName: String(safeValues[8] || '').trim(),
    memberName: String(safeValues[9] || '').trim(),
    txStatus: String(safeValues[10] || '').trim() || 'ปกติ',
    interestMonthsPaid: Math.max(0, Number(safeValues[11]) || 0),
    missedBeforePayment: Math.max(0, Number(safeValues[12]) || 0),
    missedAfterPayment: Math.max(0, Number(safeValues[13]) || 0),
    sourceSheet: String(sourceSheetName || '').trim(),
    rowIndex: Number(rowIndex) || 0
  };
}

function getIndexedRecordsByMemberId_(ss, memberId, includeInactive) {
  var spreadsheet = ss || getDatabaseSpreadsheet_();
  var normalizedMemberId = String(memberId || '').trim();
  if (!normalizedMemberId) return [];
  var indexSheet = ensureFreshTransactionIndexSheet_(spreadsheet);
  if (!indexSheet || indexSheet.getLastRow() <= 1) return [];
  var matchedRows = findRowsByExactValueInColumn_(indexSheet, 5, normalizedMemberId, 2);
  if (!matchedRows.length) return [];
  var rowMetas = readSheetRowsByIndexes_(indexSheet, matchedRows, TRANSACTION_INDEX_HEADERS.length);
  var rows = [];
  for (var i = 0; i < rowMetas.length; i++) {
    var record = buildManagedPaymentRecordFromIndexValues_(rowMetas[i].values);
    if (!includeInactive && isInactiveTransactionStatus_(record.txStatus)) continue;
    rows.push(record);
  }
  rows.sort(function(a, b) { return String(b.sortKey || '').localeCompare(String(a.sortKey || '')); });
  return rows;
}

function getIndexedRecordsByContract_(ss, contractNo, includeInactive) {
  var spreadsheet = ss || getDatabaseSpreadsheet_();
  var normalizedContract = String(contractNo || '').trim();
  if (!normalizedContract) return [];
  var indexSheet = ensureFreshTransactionIndexSheet_(spreadsheet);
  if (!indexSheet || indexSheet.getLastRow() <= 1) return [];
  var matchedRows = findRowsByExactValueInColumn_(indexSheet, 4, normalizedContract, 2);
  if (!matchedRows.length) return [];
  var rowMetas = readSheetRowsByIndexes_(indexSheet, matchedRows, TRANSACTION_INDEX_HEADERS.length);
  var rows = [];
  for (var i = 0; i < rowMetas.length; i++) {
    var record = buildManagedPaymentRecordFromIndexValues_(rowMetas[i].values);
    if (!includeInactive && isInactiveTransactionStatus_(record.txStatus)) continue;
    rows.push(record);
  }
  rows.sort(function(a, b) { return String(b.sortKey || '').localeCompare(String(a.sortKey || '')); });
  return rows;
}


function normalizeMemberDetailLoanItem_(loanItem) {
  if (!loanItem) return null;
  return {
    memberId: String(loanItem.memberId || '').trim(),
    contract: String(loanItem.contract || '').trim(),
    member: String(loanItem.member || '').trim(),
    amount: Number(loanItem.amount) || 0,
    interest: Number(loanItem.interest) || 0,
    balance: Number(loanItem.balance) || 0,
    status: String(loanItem.status || '').trim(),
    nextPayment: String(loanItem.nextPayment || '').trim(),
    createdAt: normalizeLoanCreatedAt_(loanItem.createdAt),
    guarantor1: String(loanItem.guarantor1 || '').trim(),
    guarantor2: String(loanItem.guarantor2 || '').trim()
  };
}

function getCachedAppDataSnapshot_() {
  var cached = getJsonCache_('appDataCache');
  if (cached && Array.isArray(cached.loans)) return cached;
  return null;
}

function readSheetRowsFromBottomFiltered_(sheet, startDataRow, width, limit, matcherFn, chunkSize) {
  var rows = [];
  if (!sheet || sheet.getLastRow() < startDataRow) return rows;

  var lastRow = sheet.getLastRow();
  var readWidth = Math.max(1, Number(width) || 1);
  var maxResults = Math.max(1, Number(limit) || 1);
  var size = Math.max(50, Number(chunkSize) || 200);
  var currentLastRow = lastRow;

  while (currentLastRow >= startDataRow && rows.length < maxResults) {
    var startRow = Math.max(startDataRow, currentLastRow - size + 1);
    var values = sheet.getRange(startRow, 1, currentLastRow - startRow + 1, readWidth).getValues();
    for (var i = values.length - 1; i >= 0 && rows.length < maxResults; i--) {
      if (!matcherFn || matcherFn(values[i])) {
        rows.push(values[i]);
      }
    }
    if (!matcherFn) break;
    currentLastRow = startRow - 1;
  }

  return rows;
}

function getApprovalQueueSummaryCached_(ss) {
  var cacheKey = 'approvalQueueSummary';
  var cached = getJsonCache_(cacheKey);
  if (cached && typeof cached.total === 'number') {
    return {
      total: Number(cached.total) || 0,
      pending: Number(cached.pending) || 0,
      approved: Number(cached.approved) || 0,
      rejected: Number(cached.rejected) || 0,
      failed: Number(cached.failed) || 0
    };
  }

  var summary = { total: 0, pending: 0, approved: 0, rejected: 0, failed: 0 };
  try {
    var approvalSheet = ensureApprovalQueueSheet_(ss || getDatabaseSpreadsheet_());
    if (approvalSheet.getLastRow() > 1) {
      var approvalValues = approvalSheet.getRange(2, 1, approvalSheet.getLastRow() - 1, APPROVAL_QUEUE_HEADERS.length).getValues();
      for (var a = 0; a < approvalValues.length; a++) {
        var ap = getApprovalRequestRecordFromValues_(approvalValues[a]);
        summary.total++;
        var key = String(ap.status || '').trim().toLowerCase();
        if (key === 'approved') summary.approved++;
        else if (key === 'rejected') summary.rejected++;
        else if (key === 'failed') summary.failed++;
        else summary.pending++;
      }
    }
  } catch (e) {}
  putJsonCache_(cacheKey, summary, 30);
  return summary;
}

function getNotificationSummaryCached_(ss) {
  var cacheKey = 'notificationSummary';
  var cached = getJsonCache_(cacheKey);
  if (cached && typeof cached.total === 'number') {
    return {
      total: Number(cached.total) || 0,
      sent: Number(cached.sent) || 0,
      failed: Number(cached.failed) || 0,
      skipped: Number(cached.skipped) || 0
    };
  }

  var summary = { total: 0, sent: 0, failed: 0, skipped: 0 };
  try {
    var nhSheet = ensureNotificationHistorySheet_(ss || getDatabaseSpreadsheet_());
    if (nhSheet.getLastRow() > 1) {
      var nhValues = nhSheet.getRange(2, 1, nhSheet.getLastRow() - 1, NOTIFICATION_HISTORY_HEADERS.length).getValues();
      for (var n = 0; n < nhValues.length; n++) {
        summary.total++;
        var st = String(nhValues[n][4] || '').trim().toUpperCase();
        if (st === 'SENT') summary.sent++;
        else if (st === 'FAILED') summary.failed++;
        else summary.skipped++;
      }
    }
  } catch (e) {}
  putJsonCache_(cacheKey, summary, 30);
  return summary;
}

function getAttachmentSummaryCached_(ss) {
  var cacheKey = 'attachmentSummary';
  var cached = getJsonCache_(cacheKey);
  if (cached && typeof cached.totalActive === 'number') {
    return { totalActive: Number(cached.totalActive) || 0 };
  }

  var summary = { totalActive: 0 };
  try {
    var atSheet = ensureAttachmentsIndexSheet_(ss || getDatabaseSpreadsheet_());
    if (atSheet.getLastRow() > 1) {
      var atValues = atSheet.getRange(2, 1, atSheet.getLastRow() - 1, ATTACHMENTS_INDEX_HEADERS.length).getValues();
      for (var x = 0; x < atValues.length; x++) {
        if (String(atValues[x][8] || '').trim().toLowerCase() === 'deleted') continue;
        summary.totalActive++;
      }
    }
  } catch (e) {}
  putJsonCache_(cacheKey, summary, 30);
  return summary;
}

function getMemberDetail(memberId) {
  requirePermission_('members.view', 'ไม่มีสิทธิ์ดูรายละเอียดสมาชิก');
  var normalizedMemberId = String(memberId || '').trim();
  if (!normalizedMemberId) return { status: 'Error', message: 'กรุณาระบุรหัสสมาชิก' };

  var cacheKey = 'memberDetail::' + normalizedMemberId;
  var cachedPayload = getJsonCache_(cacheKey);
  if (cachedPayload && cachedPayload.status === 'Success') {
    return Object.assign({ cached: true }, cachedPayload);
  }

  var ss = getDatabaseSpreadsheet_();
  var cachedAppData = getCachedAppDataSnapshot_();
  var member = null;
  var myLoans = [];
  var guaranteedLoans = [];

  if (cachedAppData) {
    if (Array.isArray(cachedAppData.members)) {
      for (var c = 0; c < cachedAppData.members.length; c++) {
        var cachedMember = cachedAppData.members[c];
        if (String(cachedMember.id || '').trim() === normalizedMemberId) {
          member = {
            id: String(cachedMember.id || '').trim(),
            name: String(cachedMember.name || '').trim(),
            status: String(cachedMember.status || '').trim()
          };
          break;
        }
      }
    }

    if (Array.isArray(cachedAppData.loans)) {
      for (var i = 0; i < cachedAppData.loans.length; i++) {
        var loanItem = normalizeMemberDetailLoanItem_(cachedAppData.loans[i]);
        if (!loanItem) continue;
        if (loanItem.memberId === normalizedMemberId) {
          myLoans.push(loanItem);
        }
        if (loanItem.guarantor1.indexOf('(' + normalizedMemberId + ')') !== -1 || loanItem.guarantor2.indexOf('(' + normalizedMemberId + ')') !== -1) {
          guaranteedLoans.push(loanItem);
        }
      }
    }
  } else {
    var memberSheet = ss.getSheetByName('Members');
    if (memberSheet && memberSheet.getLastRow() > 1) {
      var memberRows = memberSheet.getRange(2, 1, memberSheet.getLastRow() - 1, Math.max(3, memberSheet.getLastColumn())).getValues();
      for (var m = 0; m < memberRows.length; m++) {
        if (String(memberRows[m][0] || '').trim() === normalizedMemberId) {
          member = {
            id: String(memberRows[m][0] || '').trim(),
            name: String(memberRows[m][1] || '').trim(),
            status: String(memberRows[m][2] || '').trim()
          };
          break;
        }
      }
    }

    var loanSheet = ss.getSheetByName('Loans');
    if (loanSheet && loanSheet.getLastRow() > 1) {
      var loanRows = loanSheet.getRange(2, 1, loanSheet.getLastRow() - 1, 12).getValues();
      for (var r = 0; r < loanRows.length; r++) {
        var row = loanRows[r];
        var fallbackLoan = normalizeMemberDetailLoanItem_({
          memberId: row[0],
          contract: row[1],
          member: row[2],
          amount: row[3],
          interest: row[4],
          balance: row[5],
          status: row[6],
          nextPayment: row[7],
          createdAt: row[9],
          guarantor1: row[10],
          guarantor2: row[11]
        });
        if (!fallbackLoan) continue;
        if (fallbackLoan.memberId === normalizedMemberId) myLoans.push(fallbackLoan);
        if (fallbackLoan.guarantor1.indexOf('(' + normalizedMemberId + ')') !== -1 || fallbackLoan.guarantor2.indexOf('(' + normalizedMemberId + ')') !== -1) {
          guaranteedLoans.push(fallbackLoan);
        }
      }
    }
  }

  var transactions = getIndexedRecordsByMemberId_(ss, normalizedMemberId, true).slice(0, 50);
  var summary = {
    activeLoanCount: myLoans.filter(function(item) { return (Number(item.balance) || 0) > 0; }).length,
    totalLoanAmount: myLoans.reduce(function(sum, item) { return sum + (Number(item.amount) || 0); }, 0),
    totalOutstandingBalance: myLoans.reduce(function(sum, item) { return sum + (Number(item.balance) || 0); }, 0),
    guarantorForCount: guaranteedLoans.length,
    transactionCount: transactions.length
  };

  if (!member && myLoans.length > 0) {
    member = {
      id: normalizedMemberId,
      name: String(myLoans[0].member || '').trim(),
      status: ''
    };
  }

  var payload = {
    status: 'Success',
    member: member || { id: normalizedMemberId, name: '', status: '' },
    summary: summary,
    myLoans: myLoans,
    guaranteedLoans: guaranteedLoans,
    transactions: transactions
  };
  putJsonCache_(cacheKey, payload, 60);
  return payload;
}

function getLoanDetail(contractNo) {
  requirePermission_('loans.view', 'ไม่มีสิทธิ์ดูรายละเอียดสัญญา');
  var normalizedContract = String(contractNo || '').trim();
  if (!normalizedContract) return { status: 'Error', message: 'กรุณาระบุเลขที่สัญญา' };

  var cacheKey = 'loanDetail::' + normalizedContract;
  var cachedPayload = getJsonCache_(cacheKey);
  if (cachedPayload && cachedPayload.status === 'Success') {
    return Object.assign({ cached: true }, cachedPayload);
  }

  var ss = getDatabaseSpreadsheet_();
  var cachedAppData = getCachedAppDataSnapshot_();
  var loan = null;
  var borrower = null;

  if (cachedAppData) {
    if (Array.isArray(cachedAppData.loans)) {
      for (var c = 0; c < cachedAppData.loans.length; c++) {
        var cachedLoan = normalizeMemberDetailLoanItem_(cachedAppData.loans[c]);
        if (cachedLoan && cachedLoan.contract === normalizedContract) {
          loan = cachedLoan;
          break;
        }
      }
    }
    if (loan && Array.isArray(cachedAppData.members)) {
      for (var m = 0; m < cachedAppData.members.length; m++) {
        var cachedMember = cachedAppData.members[m];
        if (String(cachedMember.id || '').trim() === String(loan.memberId || '').trim()) {
          borrower = {
            id: String(cachedMember.id || '').trim(),
            name: String(cachedMember.name || '').trim(),
            status: String(cachedMember.status || '').trim()
          };
          break;
        }
      }
    }
  }

  if (!loan) {
    var loanSheet = ss.getSheetByName('Loans');
    if (loanSheet && loanSheet.getLastRow() > 1) {
      var loanRows = loanSheet.getRange(2, 1, loanSheet.getLastRow() - 1, 12).getValues();
      for (var i = 0; i < loanRows.length; i++) {
        var row = loanRows[i];
        if (String(row[1] || '').trim() === normalizedContract) {
          loan = normalizeMemberDetailLoanItem_({
            memberId: row[0],
            contract: row[1],
            member: row[2],
            amount: row[3],
            interest: row[4],
            balance: row[5],
            status: row[6],
            nextPayment: row[7],
            createdAt: row[9],
            guarantor1: row[10],
            guarantor2: row[11]
          });
          break;
        }
      }
    }
  }
  if (!loan) return { status: 'Error', message: 'ไม่พบข้อมูลสัญญาที่เลือก' };

  if (!borrower) {
    var memberSheet = ss.getSheetByName('Members');
    if (memberSheet && memberSheet.getLastRow() > 1) {
      var memberRows = memberSheet.getRange(2, 1, memberSheet.getLastRow() - 1, Math.max(3, memberSheet.getLastColumn())).getValues();
      for (var n = 0; n < memberRows.length; n++) {
        if (String(memberRows[n][0] || '').trim() === String(loan.memberId || '').trim()) {
          borrower = {
            id: String(memberRows[n][0] || '').trim(),
            name: String(memberRows[n][1] || '').trim(),
            status: String(memberRows[n][2] || '').trim()
          };
          break;
        }
      }
    }
  }

  var transactions = getIndexedRecordsByContract_(ss, normalizedContract, true).slice(0, 100);
  var summary = {
    transactionCount: transactions.length,
    totalPrincipalPaid: 0,
    totalInterestPaid: 0,
    totalReversedPrincipal: 0,
    totalReversedInterest: 0
  };
  for (var t = 0; t < transactions.length; t++) {
    var tx = transactions[t];
    if (tx.txStatus === 'ปกติ') {
      summary.totalPrincipalPaid += Number(tx.principalPaid) || 0;
      summary.totalInterestPaid += Number(tx.interestPaid) || 0;
    } else if (tx.txStatus === 'กลับรายการ') {
      summary.totalReversedPrincipal += Number(tx.principalPaid) || 0;
      summary.totalReversedInterest += Number(tx.interestPaid) || 0;
    }
  }

  var payload = {
    status: 'Success',
    loan: loan,
    borrower: borrower || { id: String(loan.memberId || '').trim(), name: String(loan.member || '').trim(), status: '' },
    transactions: transactions,
    summary: summary
  };
  putJsonCache_(cacheKey, payload, 60);
  return payload;
}

function getAuditLogs(filterStr) {
  requirePermission_('audit.view', 'ไม่มีสิทธิ์ดู Audit Logs');
  try {
    var filter = typeof filterStr === 'string' ? JSON.parse(filterStr || '{}') : (filterStr || {});
    var limit = Math.max(1, Math.min(500, Number(filter.limit) || 150));
    var spreadsheet = getDatabaseSpreadsheet_();
    var sheet = ensureAuditLogsSheet_(spreadsheet);
    if (sheet.getLastRow() <= 1) {
      return { status: 'Success', records: [] };
    }

    var values = readSheetRowsFromBottomFiltered_(sheet, 2, AUDIT_LOGS_HEADERS.length, limit, null, Math.min(Math.max(limit, 150), 500));
    var records = values.map(function(row) {
      return {
        timestamp: String(row[0] || '').trim(),
        username: String(row[1] || '').trim(),
        actorName: String(row[2] || '').trim(),
        role: String(row[3] || '').trim(),
        action: String(row[4] || '').trim(),
        entityType: String(row[5] || '').trim(),
        entityId: String(row[6] || '').trim(),
        referenceNo: String(row[7] || '').trim(),
        beforeJson: String(row[8] || '').trim(),
        afterJson: String(row[9] || '').trim(),
        reason: String(row[10] || '').trim(),
        details: String(row[11] || '').trim()
      };
    }).filter(function(record) {
      return record.timestamp || record.action || record.entityId || record.details;
    });

    return { status: 'Success', records: records.slice(0, limit) };
  } catch (e) {
    return { status: 'Error', message: 'โหลด Audit Logs ไม่สำเร็จ: ' + e.toString(), records: [] };
  }
}

// ==========================================
// 0. HELPER FUNCTIONS
// ==========================================

function normalizeDateObjectForThai_(dateObj) {
  if (dateObj === null || dateObj === undefined || dateObj === '') return null;

  var d = new Date(dateObj);
  if (isNaN(d.getTime())) return null;

  var year = d.getFullYear();
  if (year >= 2400 && year <= 2600) {
    d = new Date(
      year - 543,
      d.getMonth(),
      d.getDate(),
      d.getHours(),
      d.getMinutes(),
      d.getSeconds(),
      d.getMilliseconds()
    );
  }

  return d;
}

function getThaiDate(dateObj) {
  if (!dateObj) dateObj = new Date();

  var d = normalizeDateObjectForThai_(dateObj);
  if (!d) {
    return { dateOnly: String(dateObj), dateTime: String(dateObj) };
  }

  var day = ("0" + d.getDate()).slice(-2);
  var month = ("0" + (d.getMonth() + 1)).slice(-2);
  var year = d.getFullYear() + 543;

  var hours = ("0" + d.getHours()).slice(-2);
  var minutes = ("0" + d.getMinutes()).slice(-2);
  var seconds = ("0" + d.getSeconds()).slice(-2);

  return {
    dateOnly: day + "/" + month + "/" + year,
    dateTime: day + "/" + month + "/" + year + " " + hours + ":" + minutes + ":" + seconds
  };
}

function normalizeLoanCreatedAt_(rawValue) {
  if (rawValue === null || rawValue === undefined || rawValue === "") {
    return "";
  }

  var parsed = parseThaiDateParts_(rawValue);
  if (parsed && parsed.dateOnly) {
    return parsed.dateOnly;
  }

  return "";
}

function createMapIndex(data, keyColIndex) {
  var map = {};
  for (var i = 0; i < data.length; i++) {
    var key = String(data[i][keyColIndex] || "").trim();
    if (key) {
      map[key] = i + 2;
    }
  }
  return map;
}

function getAppCacheVersion_() {
  var props = PropertiesService.getScriptProperties();
  var version = String(props.getProperty('APP_CACHE_VERSION') || '').trim();
  if (!version) {
    version = String(new Date().getTime());
    props.setProperty('APP_CACHE_VERSION', version);
  }
  return version;
}

function bumpAppCacheVersion_() {
  var nextVersion = String(new Date().getTime());
  PropertiesService.getScriptProperties().setProperty('APP_CACHE_VERSION', nextVersion);
  return nextVersion;
}

function buildVersionedCacheKey_(baseKey) {
  return String(baseKey || 'cache') + '::' + getAppCacheVersion_();
}

function getJsonCache_(baseKey) {
  try {
    var raw = CacheService.getScriptCache().get(buildVersionedCacheKey_(baseKey));
    return raw ? JSON.parse(raw) : null;
  } catch (e) {
    return null;
  }
}

function putJsonCache_(baseKey, payload, ttlSeconds) {
  try {
    CacheService.getScriptCache().put(buildVersionedCacheKey_(baseKey), JSON.stringify(payload), ttlSeconds || 300);
  } catch (e) {}
}

function clearAppCache_() {
  try {
    CacheService.getScriptCache().remove('appDataCache');
  } catch (e) {}
  bumpAppCacheVersion_();
}

function clearAppCache() {
  requireAdminSession_();
  clearAppCache_();
  return {
    status: 'Success',
    message: 'ล้าง cache ระบบเรียบร้อยแล้ว'
  };
}

function getPaymentMemberLookupCacheKey_(memberId) {
  return 'paymentLookupMember::' + String(memberId || '').trim();
}

function getPaymentMemberTodayCacheKey_(memberId) {
  return 'paymentTodayMember::' + String(memberId || '').trim();
}

function getPaymentInterestRateCacheKey_() {
  return 'paymentInterestRate';
}

function getPaymentInterestRateCached_(ss) {
  if (EXECUTION_PAYMENT_INTEREST_RATE_CACHE_ !== null && EXECUTION_PAYMENT_INTEREST_RATE_CACHE_ !== undefined) {
    return Number(EXECUTION_PAYMENT_INTEREST_RATE_CACHE_) || 12.0;
  }
  var cached = getJsonCache_(getPaymentInterestRateCacheKey_());
  if (cached && Number(cached.interestRate) > 0) {
    EXECUTION_PAYMENT_INTEREST_RATE_CACHE_ = Number(cached.interestRate) || 12.0;
    return EXECUTION_PAYMENT_INTEREST_RATE_CACHE_;
  }
  var spreadsheet = ss || getDatabaseSpreadsheet_();
  var settingsSheet = spreadsheet ? spreadsheet.getSheetByName('Settings') : null;
  var interestRate = settingsSheet ? (Number(getSettingValue_(settingsSheet, 'InterestRate')) || 12.0) : 12.0;
  EXECUTION_PAYMENT_INTEREST_RATE_CACHE_ = interestRate;
  putJsonCache_(getPaymentInterestRateCacheKey_(), { interestRate: interestRate }, 300);
  return interestRate;
}

function clearVersionedJsonCache_(baseKey) {
  try {
    CacheService.getScriptCache().remove(buildVersionedCacheKey_(baseKey));
  } catch (e) {}
}

var TRANSACTION_INDEX_SHEET_NAME = '_TransactionIndex';
var TRANSACTION_INDEX_HEADERS = [
  'TxId',
  'YearBE',
  'Timestamp',
  'Contract',
  'MemberId',
  'MemberName',
  'PrincipalPaid',
  'InterestPaid',
  'NewBalance',
  'Note',
  'SourceSheet',
  'RowIndex',
  'TxStatus',
  'InterestMonthsPaid',
  'SortKey',
  'MissedBeforePayment',
  'MissedAfterPayment'
];

var EXECUTION_SHEET_COLUMN_CACHE_ = {};
var EXECUTION_SPREADSHEET_SHEETS_CACHE_ = {};
var EXECUTION_THAI_DATE_PARTS_CACHE_ = {};
var EXECUTION_NORMALIZED_THAI_DATE_CACHE_ = {};
var EXECUTION_TIME_TEXT_CACHE_ = {};
var EXECUTION_ACTIVE_TRANSACTION_RECORDS_CACHE_ = {};
var EXECUTION_TRANSACTION_ARCHIVE_META_CACHE_ = {};
var EXECUTION_PAYMENT_MEMBER_LOOKUP_CACHE_ = {};
var EXECUTION_PAYMENT_MEMBER_TODAY_CACHE_ = {};
var EXECUTION_PAYMENT_INTEREST_RATE_CACHE_ = null;

function getSheetExecutionCacheKey_(sheet, columnIndex, startRow) {
  if (!sheet) return '';
  var sheetId = '';
  try {
    sheetId = String(sheet.getSheetId());
  } catch (e) {
    sheetId = String(sheet.getName() || '');
  }
  return [sheetId, String(columnIndex || 0), String(startRow || 1), String(sheet.getLastRow() || 0)].join('::');
}

function getExecutionValueCacheKey_(value) {
  if (value === null || value === undefined || value === '') return 'empty';
  if (Object.prototype.toString.call(value) === '[object Date]') {
    var timeValue = value.getTime();
    return 'date:' + (isNaN(timeValue) ? String(value) : String(timeValue));
  }
  var valueType = typeof value;
  if (valueType === 'number' || valueType === 'boolean') {
    return valueType + ':' + String(value);
  }
  return valueType + ':' + String(value).trim();
}

function invalidateSheetExecutionCaches_(sheet) {
  if (!sheet) return;
  var sheetId = '';
  try {
    sheetId = String(sheet.getSheetId());
  } catch (e) {
    sheetId = String(sheet.getName() || '');
  }
  var nextCache = {};
  for (var key in EXECUTION_SHEET_COLUMN_CACHE_) {
    if (!EXECUTION_SHEET_COLUMN_CACHE_.hasOwnProperty(key)) continue;
    if (key.indexOf(sheetId + '::') === 0) continue;
    nextCache[key] = EXECUTION_SHEET_COLUMN_CACHE_[key];
  }
  EXECUTION_SHEET_COLUMN_CACHE_ = nextCache;

  var nextTxCache = {};
  for (var txKey in EXECUTION_ACTIVE_TRANSACTION_RECORDS_CACHE_) {
    if (!EXECUTION_ACTIVE_TRANSACTION_RECORDS_CACHE_.hasOwnProperty(txKey)) continue;
    if (txKey.indexOf(sheetId + '::') === 0) continue;
    nextTxCache[txKey] = EXECUTION_ACTIVE_TRANSACTION_RECORDS_CACHE_[txKey];
  }
  EXECUTION_ACTIVE_TRANSACTION_RECORDS_CACHE_ = nextTxCache;
}

function getSpreadsheetExecutionCacheKey_(ss) {
  if (!ss) return '';
  try {
    return String(ss.getId());
  } catch (e) {
    return String(new Date().getTime());
  }
}

function invalidateSpreadsheetExecutionCaches_(ss) {
  var cacheKey = getSpreadsheetExecutionCacheKey_(ss);
  if (!cacheKey) return;
  delete EXECUTION_SPREADSHEET_SHEETS_CACHE_[cacheKey];
  delete EXECUTION_TRANSACTION_ARCHIVE_META_CACHE_[cacheKey];
  delete EXECUTION_SETTINGS_MAP_CACHE_[cacheKey];
}

function getRuntimeDatabaseReadyCacheKey_(ss) {
  return buildVersionedCacheKey_('runtimeDbReady::' + getSpreadsheetExecutionCacheKey_(ss));
}

function isRuntimeDatabaseWarm_(ss) {
  if (!ss) return false;
  try {
    return CacheService.getScriptCache().get(getRuntimeDatabaseReadyCacheKey_(ss)) === '1';
  } catch (e) {
    return false;
  }
}

function markRuntimeDatabaseWarm_(ss) {
  if (!ss) return;
  try {
    CacheService.getScriptCache().put(getRuntimeDatabaseReadyCacheKey_(ss), '1', RUNTIME_DB_READY_CACHE_TTL_SECONDS);
  } catch (e) {}
}

function getSettingsMapCached_(settingsSheet) {
  if (!settingsSheet) return {};
  var spreadsheet = settingsSheet.getParent ? settingsSheet.getParent() : null;
  var cacheKey = spreadsheet ? getSpreadsheetExecutionCacheKey_(spreadsheet) + '::settingsMap' : 'settingsMap';
  if (EXECUTION_SETTINGS_MAP_CACHE_.hasOwnProperty(cacheKey)) {
    return EXECUTION_SETTINGS_MAP_CACHE_[cacheKey];
  }

  var settingsMap = {};
  if (settingsSheet.getLastRow() > 0) {
    var data = settingsSheet.getRange(1, 1, settingsSheet.getLastRow(), 2).getValues();
    for (var i = 0; i < data.length; i++) {
      var settingKey = String(data[i][0] || '').trim();
      if (!settingKey) continue;
      settingsMap[settingKey] = data[i][1];
    }
  }

  EXECUTION_SETTINGS_MAP_CACHE_[cacheKey] = settingsMap;
  return settingsMap;
}

function getSpreadsheetSheetsCached_(ss) {
  if (!ss) return [];
  var cacheKey = getSpreadsheetExecutionCacheKey_(ss);
  if (!cacheKey) return ss.getSheets();
  if (EXECUTION_SPREADSHEET_SHEETS_CACHE_[cacheKey]) {
    return EXECUTION_SPREADSHEET_SHEETS_CACHE_[cacheKey];
  }
  var sheets = ss.getSheets();
  EXECUTION_SPREADSHEET_SHEETS_CACHE_[cacheKey] = sheets;
  return sheets;
}

function getExactRowIndexesByColumnValueCached_(sheet, columnIndex, targetValue, startRow) {
  if (!sheet || !columnIndex || targetValue === undefined || targetValue === null) return [];
  var normalizedStartRow = Math.max(1, Number(startRow) || 1);
  var lastRow = sheet.getLastRow();
  if (lastRow < normalizedStartRow) return [];

  var cacheKey = getSheetExecutionCacheKey_(sheet, columnIndex, normalizedStartRow);
  var bucket = EXECUTION_SHEET_COLUMN_CACHE_[cacheKey];
  if (!bucket) {
    var values = sheet.getRange(normalizedStartRow, columnIndex, lastRow - normalizedStartRow + 1, 1).getDisplayValues();
    bucket = {};
    for (var i = 0; i < values.length; i++) {
      var key = String(values[i][0] || '').trim();
      if (!key) continue;
      if (!bucket[key]) bucket[key] = [];
      bucket[key].push(normalizedStartRow + i);
    }
    EXECUTION_SHEET_COLUMN_CACHE_[cacheKey] = bucket;
  }

  var normalizedTarget = String(targetValue || '').trim();
  return (bucket[normalizedTarget] || []).slice();
}

function getTransactionArchiveSheetsMetaCached_(ss) {
  if (!ss) return [];
  var cacheKey = getSpreadsheetExecutionCacheKey_(ss);
  if (cacheKey && EXECUTION_TRANSACTION_ARCHIVE_META_CACHE_[cacheKey]) {
    return EXECUTION_TRANSACTION_ARCHIVE_META_CACHE_[cacheKey];
  }

  var sheets = getSpreadsheetSheetsCached_(ss);
  var result = [];
  for (var i = 0; i < sheets.length; i++) {
    var meta = parseTransactionsArchiveSheetMeta_(sheets[i].getName());
    if (!meta) continue;
    result.push({ sheet: sheets[i], meta: meta });
  }

  if (cacheKey) {
    EXECUTION_TRANSACTION_ARCHIVE_META_CACHE_[cacheKey] = result;
  }
  return result;
}

function getTransactionIndexVersion_() {
  return String(PropertiesService.getScriptProperties().getProperty('TRANSACTION_INDEX_VERSION') || '').trim();
}

function setTransactionIndexVersion_(version) {
  PropertiesService.getScriptProperties().setProperty('TRANSACTION_INDEX_VERSION', String(version || '').trim());
}

function syncTransactionIndexVersionToCache_() {
  setTransactionIndexVersion_(getAppCacheVersion_());
}


var PAYMENT_SEARCH_INDEX_SHEET_NAME = '_PaymentSearchIndex';
var PAYMENT_SEARCH_INDEX_HEADERS = [
  'MemberId',
  'Contract',
  'MemberName',
  'Amount',
  'InterestRate',
  'Balance',
  'Status',
  'NextPayment',
  'MissedInterestMonths',
  'CurrentMonthInterestPaid',
  'ReportMissedInterestMonths',
  'CurrentMonthRequiredInstallment',
  'MaxInterestInstallments',
  'MonthlyInterestDue',
  'CreatedAt',
  'Guarantor1',
  'Guarantor2',
  'UpdatedAt'
];

var PAYMENT_TODAY_INDEX_SHEET_NAME = '_PaymentTodayIndex';
var PAYMENT_TODAY_INDEX_HEADERS = [
  'DateOnly',
  'MemberId',
  'TxId',
  'Timestamp',
  'Contract',
  'PrincipalPaid',
  'InterestPaid',
  'NewBalance',
  'Note',
  'MemberName',
  'TxStatus',
  'InterestMonthsPaid',
  'SortKey'
];

function getPaymentSearchIndexVersion_() {
  return String(PropertiesService.getScriptProperties().getProperty('PAYMENT_SEARCH_INDEX_VERSION') || '').trim();
}

function setPaymentSearchIndexVersion_(version) {
  PropertiesService.getScriptProperties().setProperty('PAYMENT_SEARCH_INDEX_VERSION', String(version || '').trim());
}

function syncPaymentSearchIndexVersionToCache_() {
  setPaymentSearchIndexVersion_(getAppCacheVersion_());
}

function getPaymentTodayIndexVersion_() {
  return String(PropertiesService.getScriptProperties().getProperty('PAYMENT_TODAY_INDEX_VERSION') || '').trim();
}

function setPaymentTodayIndexVersion_(version) {
  PropertiesService.getScriptProperties().setProperty('PAYMENT_TODAY_INDEX_VERSION', String(version || '').trim());
}

function syncPaymentTodayIndexVersionToCache_() {
  setPaymentTodayIndexVersion_(getAppCacheVersion_());
}

function getPaymentSearchIndexSheet_(ss) {
  var spreadsheet = ss || getDatabaseSpreadsheet_();
  var sheet = spreadsheet.getSheetByName(PAYMENT_SEARCH_INDEX_SHEET_NAME)
    || createSheetSafely_(spreadsheet, PAYMENT_SEARCH_INDEX_SHEET_NAME, 2, PAYMENT_SEARCH_INDEX_HEADERS.length, 'เตรียมชีตดัชนีค้นหารับชำระ');

  ensureSheetGridSizeSafely_(sheet, Math.max(sheet.getMaxRows(), 2), PAYMENT_SEARCH_INDEX_HEADERS.length, 'เตรียมชีตดัชนีค้นหารับชำระ');
  sheet.getRange(1, 1, 1, PAYMENT_SEARCH_INDEX_HEADERS.length)
    .setValues([PAYMENT_SEARCH_INDEX_HEADERS])
    .setFontWeight('bold')
    .setBackground('#dbeafe');
  if (sheet.getFrozenRows() < 1) sheet.setFrozenRows(1);
  sheet.hideSheet();
  return sheet;
}

function getPaymentTodayIndexSheet_(ss) {
  var spreadsheet = ss || getDatabaseSpreadsheet_();
  var sheet = spreadsheet.getSheetByName(PAYMENT_TODAY_INDEX_SHEET_NAME)
    || createSheetSafely_(spreadsheet, PAYMENT_TODAY_INDEX_SHEET_NAME, 2, PAYMENT_TODAY_INDEX_HEADERS.length, 'เตรียมชีตดัชนีรายการวันนี้สำหรับรับชำระ');

  ensureSheetGridSizeSafely_(sheet, Math.max(sheet.getMaxRows(), 2), PAYMENT_TODAY_INDEX_HEADERS.length, 'เตรียมชีตดัชนีรายการวันนี้สำหรับรับชำระ');
  sheet.getRange(1, 1, 1, PAYMENT_TODAY_INDEX_HEADERS.length)
    .setValues([PAYMENT_TODAY_INDEX_HEADERS])
    .setFontWeight('bold')
    .setBackground('#dbeafe');
  if (sheet.getFrozenRows() < 1) sheet.setFrozenRows(1);
  sheet.hideSheet();
  return sheet;
}

function isPaymentSearchLoanActiveRow_(loanRow) {
  var row = (loanRow || []).slice();
  var contractNo = String(row[1] || '').trim();
  var balance = Number(row[5]) || 0;
  var statusText = String(row[6] || '').trim();
  if (!contractNo) return false;
  if (balance <= 0) return false;
  if (statusText === 'ปิดบัญชี') return false;
  if (statusText.indexOf('กลบหนี้') !== -1) return false;
  return true;
}

function collectCurrentMonthInterestPaidByContractFromCurrentSheets_(ss, contractFilterSet) {
  var spreadsheet = ss || getDatabaseSpreadsheet_();
  var now = new Date();
  var nowParts = parseThaiDateParts_(now) || { month: now.getMonth() + 1, yearBe: now.getFullYear() + 543 };
  var currentMonth = Number(nowParts.month) || (now.getMonth() + 1);
  var currentYearBe = Number(nowParts.yearBe) || (now.getFullYear() + 543);
  var map = {};
  var currentSheets = listCurrentTransactionSheets_(spreadsheet);

  for (var s = 0; s < currentSheets.length; s++) {
    var sheet = currentSheets[s];
    if (!sheet || sheet.getLastRow() <= 1) continue;
    ensureTransactionsSheetSchema_(sheet);
    var readCols = Math.min(Math.max(sheet.getLastColumn(), 14), 14);
    var values = sheet.getRange(2, 1, sheet.getLastRow() - 1, readCols).getValues();
    for (var r = 0; r < values.length; r++) {
      var row = values[r];
      var contractNo = String(row[2] || '').trim();
      if (!contractNo) continue;
      if (contractFilterSet && !contractFilterSet[contractNo]) continue;
      if (isInactiveTransactionStatus_(String(row[10] || '').trim())) continue;
      if ((Number(row[5]) || 0) <= 0) continue;
      var dateParts = parseThaiDateParts_(row[1]);
      if (!dateParts) continue;
      if (Number(dateParts.yearBe) !== currentYearBe || Number(dateParts.month) !== currentMonth) continue;
      map[contractNo] = true;
    }
  }

  return map;
}

function buildPaymentSearchIndexRowFromLoanValues_(loanRowValues, currentMonthInterestPaidMap, updatedAtText, includeCurrentMonthInstallment) {
  var row = (loanRowValues || []).slice();
  while (row.length < 12) row.push('');
  var memberId = String(row[0] || '').trim();
  var contractNo = String(row[1] || '').trim();
  var memberName = String(row[2] || '').trim();
  var amount = Number(row[3]) || 0;
  var interestRate = Number(row[4]);
  if (!isFinite(interestRate) || interestRate < 0) interestRate = 12.0;
  var balance = Number(row[5]) || 0;
  var statusText = deriveLoanStatusFromState_(balance, Math.max(0, Number(row[8]) || 0), String(row[6] || '').trim());
  var nextPayment = String(row[7] || '').trim();
  var missedInterestMonths = Math.max(0, Number(row[8]) || 0);
  var currentMonthInterestPaid = !!(currentMonthInterestPaidMap && currentMonthInterestPaidMap[contractNo]);
  var shouldRequireCurrentMonthInstallment = !!includeCurrentMonthInstallment;
  var currentMonthRequiredInstallment = shouldRequireCurrentMonthInstallment && !currentMonthInterestPaid ? 1 : 0;
  var reportMissedInterestMonths = missedInterestMonths + currentMonthRequiredInstallment;
  var maxInterestInstallments = Math.max(1, reportMissedInterestMonths);
  var monthlyInterestDue = Math.floor(Math.max(0, balance) * (interestRate / 100) / 12);

  return [
    memberId,
    contractNo,
    memberName,
    amount,
    interestRate,
    balance,
    statusText,
    nextPayment,
    missedInterestMonths,
    currentMonthInterestPaid ? '1' : '0',
    reportMissedInterestMonths,
    currentMonthRequiredInstallment,
    maxInterestInstallments,
    monthlyInterestDue,
    normalizeLoanCreatedAt_(row[9]),
    String(row[10] || '').trim(),
    String(row[11] || '').trim(),
    String(updatedAtText || getThaiDate(new Date()).dateTime)
  ];
}

function buildPaymentLoanFromIndexValues_(rowValues) {
  var row = (rowValues || []).slice();
  while (row.length < PAYMENT_SEARCH_INDEX_HEADERS.length) row.push('');
  return {
    memberId: String(row[0] || '').trim(),
    contract: String(row[1] || '').trim(),
    member: String(row[2] || '').trim(),
    amount: Number(row[3]) || 0,
    interest: Number(row[4]) || 0,
    balance: Number(row[5]) || 0,
    status: String(row[6] || '').trim(),
    nextPayment: String(row[7] || '').trim(),
    missedInterestMonths: Math.max(0, Number(row[8]) || 0),
    currentMonthInterestPaid: String(row[9] || '').trim() === '1',
    reportMissedInterestMonths: Math.max(0, Number(row[10]) || 0),
    currentMonthRequiredInstallment: Math.max(0, Number(row[11]) || 0),
    maxInterestInstallments: Math.max(1, Number(row[12]) || 0),
    monthlyInterestDue: Math.max(0, Number(row[13]) || 0),
    createdAt: String(row[14] || '').trim(),
    guarantor1: String(row[15] || '').trim(),
    guarantor2: String(row[16] || '').trim()
  };
}

function rebuildPaymentSearchIndexSheet_(ss) {
  var spreadsheet = ss || getDatabaseSpreadsheet_();
  var indexSheet = getPaymentSearchIndexSheet_(spreadsheet);
  invalidateSheetExecutionCaches_(indexSheet);
  if (indexSheet.getLastRow() > 1) {
    indexSheet.getRange(2, 1, indexSheet.getLastRow() - 1, Math.max(indexSheet.getLastColumn(), PAYMENT_SEARCH_INDEX_HEADERS.length)).clearContent();
  }

  var loanSheet = spreadsheet.getSheetByName('Loans');
  var rowsToWrite = [];
  if (loanSheet && loanSheet.getLastRow() > 1) {
    var loanRows = loanSheet.getRange(2, 1, loanSheet.getLastRow() - 1, 12).getValues();
    var activeContracts = {};
    for (var i = 0; i < loanRows.length; i++) {
      if (!isPaymentSearchLoanActiveRow_(loanRows[i])) continue;
      activeContracts[String(loanRows[i][1] || '').trim()] = true;
    }
    var currentMonthInterestPaidMap = collectCurrentMonthInterestPaidByContractFromCurrentSheets_(spreadsheet, activeContracts);
    var settingsSheet = spreadsheet.getSheetByName('Settings');
    var includeCurrentMonthInstallment = hasReachedOperatingDayForDate_(settingsSheet, new Date());
    var updatedAtText = getThaiDate(new Date()).dateTime;
    for (var r = 0; r < loanRows.length; r++) {
      if (!isPaymentSearchLoanActiveRow_(loanRows[r])) continue;
      rowsToWrite.push(buildPaymentSearchIndexRowFromLoanValues_(loanRows[r], currentMonthInterestPaidMap, updatedAtText, includeCurrentMonthInstallment));
    }
    rowsToWrite.sort(function(a, b) {
      var memberCompare = String(a[0] || '').localeCompare(String(b[0] || ''), 'th', { numeric: true, sensitivity: 'base' });
      if (memberCompare !== 0) return memberCompare;
      return String(a[1] || '').localeCompare(String(b[1] || ''), 'th', { numeric: true, sensitivity: 'base' });
    });
  }

  var chunkSize = 2000;
  for (var start = 0; start < rowsToWrite.length; start += chunkSize) {
    var chunk = rowsToWrite.slice(start, start + chunkSize);
    indexSheet.getRange(2 + start, 1, chunk.length, PAYMENT_SEARCH_INDEX_HEADERS.length).setValues(chunk);
  }

  setPlainTextColumnFormat_(indexSheet, [1, 2, 3, 8, 15, 16, 17, 18]);
  indexSheet.hideSheet();
  invalidateSheetExecutionCaches_(indexSheet);
  setPaymentSearchIndexVersion_(getAppCacheVersion_());
  return indexSheet;
}

function ensureFreshPaymentSearchIndexSheet_(ss, forceRebuild) {
  var spreadsheet = ss || getDatabaseSpreadsheet_();
  var desiredVersion = getAppCacheVersion_();
  var indexSheet = getPaymentSearchIndexSheet_(spreadsheet);
  var currentVersion = getPaymentSearchIndexVersion_();

  if (!forceRebuild && currentVersion === desiredVersion && indexSheet.getLastRow() > 1) {
    return indexSheet;
  }

  var lock = LockService.getScriptLock();
  var lockAcquired = false;
  try {
    lock.waitLock(30000);
    lockAcquired = true;
    currentVersion = getPaymentSearchIndexVersion_();
    indexSheet = getPaymentSearchIndexSheet_(spreadsheet);
    if (!forceRebuild && currentVersion === desiredVersion && indexSheet.getLastRow() > 1) {
      return indexSheet;
    }
    return rebuildPaymentSearchIndexSheet_(spreadsheet);
  } finally {
    if (lockAcquired) lock.releaseLock();
  }
}

function syncPaymentSearchIndexForContracts_(ss, contractNos) {
  var spreadsheet = ss || getDatabaseSpreadsheet_();
  var indexSheet = getPaymentSearchIndexSheet_(spreadsheet);
  var normalizedContracts = (contractNos || []).map(function(item) {
    return String(item || '').trim();
  }).filter(Boolean);

  if (normalizedContracts.length === 0) {
    rebuildPaymentSearchIndexSheet_(spreadsheet);
    return;
  }

  var loanSheet = spreadsheet.getSheetByName('Loans');
  var currentMonthInterestPaidMap = collectCurrentMonthInterestPaidByContractFromCurrentSheets_(spreadsheet, normalizedContracts.reduce(function(map, contractNo) {
    map[contractNo] = true;
    return map;
  }, {}));
  var settingsSheet = spreadsheet.getSheetByName('Settings');
  var includeCurrentMonthInstallment = hasReachedOperatingDayForDate_(settingsSheet, new Date());
  var rowsToAppend = [];
  var rowsToDelete = [];
  var updatedAtText = getThaiDate(new Date()).dateTime;

  for (var i = 0; i < normalizedContracts.length; i++) {
    var contractNo = normalizedContracts[i];
    var matchedRows = findRowsByExactValueInColumn_(indexSheet, 2, contractNo, 2);
    for (var d = 0; d < matchedRows.length; d++) rowsToDelete.push(matchedRows[d]);

    if (!loanSheet) continue;
    var loanRowIndex = findRowByExactValue(loanSheet, 2, contractNo);
    if (!loanRowIndex) continue;
    var loanRow = loanSheet.getRange(loanRowIndex, 1, 1, 12).getValues()[0];
    if (!isPaymentSearchLoanActiveRow_(loanRow)) continue;
    rowsToAppend.push(buildPaymentSearchIndexRowFromLoanValues_(loanRow, currentMonthInterestPaidMap, updatedAtText, includeCurrentMonthInstallment));
  }

  if (rowsToDelete.length > 0) {
    deleteRowsByIndexes_(indexSheet, rowsToDelete);
  }
  if (rowsToAppend.length > 0) {
    appendRowsSafely_(indexSheet, rowsToAppend, 'อัปเดตดัชนีค้นหารับชำระ');
  }

  setPlainTextColumnFormat_(indexSheet, [1, 2, 3, 8, 15, 16, 17, 18]);
  indexSheet.hideSheet();
  invalidateSheetExecutionCaches_(indexSheet);
}

function buildPaymentTodayIndexRowFromValues_(rowIndex, rowValues) {
  var row = (rowValues || []).slice();
  while (row.length < 14) row.push('');
  var timestamp = normalizeThaiDateCellValue_(row[1], true);
  var dateOnly = String(timestamp || '').split(' ')[0] || '';
  var sortKey = buildTransactionOrderKey_(row, rowIndex || 0);
  return [
    dateOnly,
    String(row[3] || '').trim(),
    String(row[0] || '').trim(),
    timestamp,
    String(row[2] || '').trim(),
    Number(row[4]) || 0,
    Number(row[5]) || 0,
    Number(row[6]) || 0,
    String(row[7] || '').trim(),
    String(row[9] || '').trim(),
    String(row[10] || '').trim() || 'ปกติ',
    Math.max(0, Number(row[11]) || 0),
    sortKey
  ];
}

function syncPaymentTodayIndexEntries_(ss, entries) {
  var spreadsheet = ss || getDatabaseSpreadsheet_();
  var normalizedEntries = (entries || []).map(function(entry) {
    var rowValues = entry && entry.rowValues ? entry.rowValues.slice() : null;
    if (rowValues) {
      while (rowValues.length < 14) rowValues.push('');
    }
    return {
      txId: String((entry && entry.txId) || (rowValues && rowValues[0]) || '').trim(),
      rowValues: rowValues,
      rowIndex: Number(entry && entry.rowIndex) || 0
    };
  }).filter(function(entry) {
    return !!entry.txId;
  });

  if (!normalizedEntries.length) return null;

  var indexSheet = getPaymentTodayIndexSheet_(spreadsheet);
  var rowsToDeleteMap = {};
  var rowsToAppend = [];
  var todayThaiDate = getThaiDate(new Date()).dateOnly;

  for (var i = 0; i < normalizedEntries.length; i++) {
    var entry = normalizedEntries[i];
    var matchedRows = findExactRowsByTextFinderInColumn_(indexSheet, 3, entry.txId, 2);
    for (var d = 0; d < matchedRows.length; d++) {
      rowsToDeleteMap[matchedRows[d]] = true;
    }

    var row = entry.rowValues;
    if (!row) continue;
    if (isInactiveTransactionStatus_(String(row[10] || '').trim())) continue;

    var timestamp = normalizeThaiDateCellValue_(row[1], true);
    var dateOnly = String(timestamp || '').split(' ')[0] || '';
    if (dateOnly !== todayThaiDate) continue;

    rowsToAppend.push(buildPaymentTodayIndexRowFromValues_(entry.rowIndex, row));
  }

  var rowsToDelete = Object.keys(rowsToDeleteMap).map(function(key) {
    return Number(key) || 0;
  }).filter(function(rowIndex) {
    return rowIndex > 1;
  }).sort(function(a, b) {
    return a - b;
  });

  if (rowsToDelete.length) {
    deleteRowsByIndexes_(indexSheet, rowsToDelete);
  }
  if (rowsToAppend.length) {
    appendRowsSafely_(indexSheet, rowsToAppend, 'อัปเดตดัชนีรายการวันนี้');
  }

  setPlainTextColumnFormat_(indexSheet, [1, 2, 3, 4, 5, 9, 10, 11, 13]);
  indexSheet.hideSheet();
  invalidateSheetExecutionCaches_(indexSheet);
  return indexSheet;
}

function rebuildPaymentTodayIndexSheet_(ss) {
  var spreadsheet = ss || getDatabaseSpreadsheet_();
  var indexSheet = getPaymentTodayIndexSheet_(spreadsheet);
  invalidateSheetExecutionCaches_(indexSheet);
  if (indexSheet.getLastRow() > 1) {
    indexSheet.getRange(2, 1, indexSheet.getLastRow() - 1, Math.max(indexSheet.getLastColumn(), PAYMENT_TODAY_INDEX_HEADERS.length)).clearContent();
  }

  var todayThaiDate = getThaiDate(new Date()).dateOnly;
  var rowsToWrite = [];
  var currentSheets = listCurrentTransactionSheets_(spreadsheet);
  for (var s = 0; s < currentSheets.length; s++) {
    var sheet = currentSheets[s];
    if (!sheet || sheet.getLastRow() <= 1) continue;
    ensureTransactionsSheetSchema_(sheet);
    var readCols = Math.min(Math.max(sheet.getLastColumn(), 14), 14);
    var values = sheet.getRange(2, 1, sheet.getLastRow() - 1, readCols).getValues();
    for (var r = 0; r < values.length; r++) {
      var row = values[r];
      if (!String(row[0] || '').trim()) continue;
      if (isInactiveTransactionStatus_(String(row[10] || '').trim())) continue;
      var timestamp = normalizeThaiDateCellValue_(row[1], true);
      var dateOnly = String(timestamp || '').split(' ')[0] || '';
      if (dateOnly !== todayThaiDate) continue;
      rowsToWrite.push(buildPaymentTodayIndexRowFromValues_(r + 2, row));
    }
  }

  rowsToWrite.sort(function(a, b) {
    return String(b[12] || '').localeCompare(String(a[12] || ''));
  });

  var chunkSize = 2000;
  for (var start = 0; start < rowsToWrite.length; start += chunkSize) {
    var chunk = rowsToWrite.slice(start, start + chunkSize);
    indexSheet.getRange(2 + start, 1, chunk.length, PAYMENT_TODAY_INDEX_HEADERS.length).setValues(chunk);
  }

  setPlainTextColumnFormat_(indexSheet, [1, 2, 3, 4, 5, 9, 10, 11, 13]);
  indexSheet.hideSheet();
  invalidateSheetExecutionCaches_(indexSheet);
  setPaymentTodayIndexVersion_(getAppCacheVersion_());
  return indexSheet;
}

function ensureFreshPaymentTodayIndexSheet_(ss, forceRebuild) {
  var spreadsheet = ss || getDatabaseSpreadsheet_();
  var desiredVersion = getAppCacheVersion_();
  var indexSheet = getPaymentTodayIndexSheet_(spreadsheet);
  var currentVersion = getPaymentTodayIndexVersion_();

  if (!forceRebuild && currentVersion === desiredVersion && indexSheet.getLastRow() > 1) {
    return indexSheet;
  }

  var lock = LockService.getScriptLock();
  var lockAcquired = false;
  try {
    lock.waitLock(30000);
    lockAcquired = true;
    currentVersion = getPaymentTodayIndexVersion_();
    indexSheet = getPaymentTodayIndexSheet_(spreadsheet);
    if (!forceRebuild && currentVersion === desiredVersion && indexSheet.getLastRow() > 1) {
      return indexSheet;
    }
    return rebuildPaymentTodayIndexSheet_(spreadsheet);
  } finally {
    if (lockAcquired) lock.releaseLock();
  }
}

function getIndexedPaymentLoansByMemberId_(ss, memberId) {
  var spreadsheet = ss || getDatabaseSpreadsheet_();
  var normalizedMemberId = String(memberId || '').trim();
  if (!normalizedMemberId) return [];

  var indexSheet = ensureFreshPaymentSearchIndexSheet_(spreadsheet);
  if (!indexSheet || indexSheet.getLastRow() <= 1) return [];

  var matchedRows = findExactRowsByTextFinderInColumn_(indexSheet, 1, normalizedMemberId, 2);
  if (!matchedRows.length) return [];

  var rowMetas = readSheetRowsByIndexes_(indexSheet, matchedRows, PAYMENT_SEARCH_INDEX_HEADERS.length);
  var loans = [];
  for (var i = 0; i < rowMetas.length; i++) {
    loans.push(buildPaymentLoanFromIndexValues_(rowMetas[i].values));
  }
  loans.sort(function(a, b) {
    var balanceDiff = (Number(b.balance) || 0) - (Number(a.balance) || 0);
    if (balanceDiff !== 0) return balanceDiff;
    return String(a.contract || '').localeCompare(String(b.contract || ''), 'th', { numeric: true, sensitivity: 'base' });
  });
  return loans;
}

function buildPaymentTodayTransactionFromIndexValues_(rowValues) {
  var row = (rowValues || []).slice();
  while (row.length < PAYMENT_TODAY_INDEX_HEADERS.length) row.push('');
  return {
    id: String(row[2] || '').trim(),
    timestamp: String(row[3] || '').trim(),
    contract: String(row[4] || '').trim(),
    memberId: String(row[1] || '').trim(),
    principalPaid: Number(row[5]) || 0,
    interestPaid: Number(row[6]) || 0,
    newBalance: Number(row[7]) || 0,
    note: String(row[8] || '').trim(),
    memberName: String(row[9] || '').trim(),
    txStatus: String(row[10] || '').trim() || 'ปกติ',
    interestMonthsPaid: Math.max(0, Number(row[11]) || 0)
  };
}

function getIndexedTodayTransactionsByMemberId_(ss, memberId) {
  var spreadsheet = ss || getDatabaseSpreadsheet_();
  var normalizedMemberId = String(memberId || '').trim();
  if (!normalizedMemberId) return [];

  var indexSheet = ensureFreshPaymentTodayIndexSheet_(spreadsheet);
  if (!indexSheet || indexSheet.getLastRow() <= 1) return [];

  var matchedRows = findRowsByExactValueInColumn_(indexSheet, 2, normalizedMemberId, 2);
  if (!matchedRows.length) return [];

  var rowMetas = readSheetRowsByIndexes_(indexSheet, matchedRows, PAYMENT_TODAY_INDEX_HEADERS.length);
  var rows = [];
  for (var i = 0; i < rowMetas.length; i++) {
    rows.push(buildPaymentTodayTransactionFromIndexValues_(rowMetas[i].values));
  }
  rows.sort(function(a, b) {
    return String((b && b.timestamp) || '').localeCompare(String((a && a.timestamp) || ''));
  });
  return rows;
}



function hasActivePaymentForContractOnThaiDate_(ss, contractNo, thaiDate) {
  var spreadsheet = ss || getDatabaseSpreadsheet_();
  var normalizedContract = String(contractNo || '').trim();
  var normalizedThaiDate = String(thaiDate || '').trim();
  if (!spreadsheet || !normalizedContract || !normalizedThaiDate) return false;

  var currentSheets = listCurrentTransactionSheets_(spreadsheet);
  for (var s = 0; s < currentSheets.length; s++) {
    var sheet = currentSheets[s];
    if (!sheet || sheet.getLastRow() <= 1) continue;

    ensureTransactionsSheetSchema_(sheet);
    var readCols = Math.min(Math.max(sheet.getLastColumn(), 11), 14);
    var values = sheet.getRange(2, 1, sheet.getLastRow() - 1, readCols).getValues();
    for (var r = 0; r < values.length; r++) {
      var row = values[r];
      if (String(row[2] || '').trim() !== normalizedContract) continue;
      if (isInactiveTransactionStatus_(String(row[10] || '').trim())) continue;

      var txTimestamp = normalizeThaiDateCellValue_(row[1], true);
      var txThaiDate = String(txTimestamp || '').split(' ')[0] || '';
      if (txThaiDate === normalizedThaiDate) {
        return true;
      }
    }
  }

  return false;
}

function findExactRowsByTextFinderInColumn_(sheet, columnIndex, targetValue, startRow) {
  if (!sheet || !columnIndex || targetValue === undefined || targetValue === null) return [];
  var normalizedStartRow = Math.max(1, Number(startRow) || 1);
  var lastRow = sheet.getLastRow();
  if (lastRow < normalizedStartRow) return [];

  var text = String(targetValue || '').trim();
  if (!text) return [];

  var range = sheet.getRange(normalizedStartRow, columnIndex, lastRow - normalizedStartRow + 1, 1);
  var finder = range.createTextFinder(text).matchEntireCell(true);
  var matches = finder.findAll() || [];
  var rows = [];
  for (var i = 0; i < matches.length; i++) {
    var rowIndex = Number(matches[i].getRow()) || 0;
    if (rowIndex >= normalizedStartRow) rows.push(rowIndex);
  }
  rows.sort(function(a, b) { return a - b; });
  return rows;
}

function getIndexedPaymentLoansByMemberIdFast_(ss, memberId) {
  var spreadsheet = ss || getDatabaseSpreadsheet_();
  var normalizedMemberId = String(memberId || '').trim();
  if (!normalizedMemberId) return [];

  if (EXECUTION_PAYMENT_MEMBER_LOOKUP_CACHE_[normalizedMemberId]) {
    return EXECUTION_PAYMENT_MEMBER_LOOKUP_CACHE_[normalizedMemberId].slice();
  }

  var desiredVersion = getAppCacheVersion_();
  var indexSheet = getPaymentSearchIndexSheet_(spreadsheet);
  var currentVersion = getPaymentSearchIndexVersion_();
  if (!indexSheet || indexSheet.getLastRow() <= 1 || currentVersion !== desiredVersion) {
    indexSheet = ensureFreshPaymentSearchIndexSheet_(spreadsheet, currentVersion !== desiredVersion);
  }
  if (!indexSheet || indexSheet.getLastRow() <= 1) return [];

  var matchedRows = findExactRowsByTextFinderInColumn_(indexSheet, 1, normalizedMemberId, 2);
  if (!matchedRows.length) {
    EXECUTION_PAYMENT_MEMBER_LOOKUP_CACHE_[normalizedMemberId] = [];
    return [];
  }

  var rowMetas = readSheetRowsByIndexes_(indexSheet, matchedRows, PAYMENT_SEARCH_INDEX_HEADERS.length);
  var loans = [];
  for (var i = 0; i < rowMetas.length; i++) {
    loans.push(buildPaymentLoanFromIndexValues_(rowMetas[i].values));
  }
  loans.sort(function(a, b) {
    var balanceDiff = (Number(b.balance) || 0) - (Number(a.balance) || 0);
    if (balanceDiff !== 0) return balanceDiff;
    return String(a.contract || '').localeCompare(String(b.contract || ''), 'th', { numeric: true, sensitivity: 'base' });
  });
  EXECUTION_PAYMENT_MEMBER_LOOKUP_CACHE_[normalizedMemberId] = loans.slice();
  return loans;
}

function getIndexedTodayTransactionsByMemberIdFast_(ss, memberId) {
  var spreadsheet = ss || getDatabaseSpreadsheet_();
  var normalizedMemberId = String(memberId || '').trim();
  if (!normalizedMemberId) return [];

  if (EXECUTION_PAYMENT_MEMBER_TODAY_CACHE_[normalizedMemberId]) {
    return EXECUTION_PAYMENT_MEMBER_TODAY_CACHE_[normalizedMemberId].slice();
  }

  var desiredVersion = getAppCacheVersion_();
  var indexSheet = getPaymentTodayIndexSheet_(spreadsheet);
  var currentVersion = getPaymentTodayIndexVersion_();
  if (!indexSheet || indexSheet.getLastRow() <= 1 || currentVersion !== desiredVersion) {
    indexSheet = ensureFreshPaymentTodayIndexSheet_(spreadsheet, currentVersion !== desiredVersion);
  }
  if (!indexSheet || indexSheet.getLastRow() <= 1) return [];

  var matchedRows = findExactRowsByTextFinderInColumn_(indexSheet, 2, normalizedMemberId, 2);
  if (!matchedRows.length) {
    EXECUTION_PAYMENT_MEMBER_TODAY_CACHE_[normalizedMemberId] = [];
    return [];
  }

  var rowMetas = readSheetRowsByIndexes_(indexSheet, matchedRows, PAYMENT_TODAY_INDEX_HEADERS.length);
  var rows = [];
  for (var i = 0; i < rowMetas.length; i++) {
    rows.push(buildPaymentTodayTransactionFromIndexValues_(rowMetas[i].values));
  }
  rows.sort(function(a, b) {
    return String((b && b.timestamp) || '').localeCompare(String((a && a.timestamp) || ''));
  });
  EXECUTION_PAYMENT_MEMBER_TODAY_CACHE_[normalizedMemberId] = rows.slice();
  return rows;
}


function isDuplicatePaymentPayloadConsistent_(existingValues, paymentData) {
  var row = (existingValues || []).slice();
  var payload = paymentData || {};

  var existingContract = String(row[2] || '').trim();
  var requestedContract = String(payload.contract || existingContract).trim();
  if (existingContract !== requestedContract) return false;

  var principalPaid = Number(payload.principalPaid);
  var interestPaid = Number(payload.interestPaid);
  var newBalance = Number(payload.newBalance);

  if (isFinite(principalPaid) && Math.abs((Number(row[4]) || 0) - principalPaid) > 0.0001) return false;
  if (isFinite(interestPaid) && Math.abs((Number(row[5]) || 0) - interestPaid) > 0.0001) return false;
  if (isFinite(newBalance) && Math.abs((Number(row[6]) || 0) - newBalance) > 0.0001) return false;

  return true;
}

function sanitizeManagedPaymentNote_(noteValue, fallbackNote) {
  var note = String(noteValue || '').trim();
  if (!note && fallbackNote) note = String(fallbackNote || '').trim();
  return note.substring(0, 120);
}

function writeSheetRowsBatchByIndexes_(sheet, rowUpdates) {
  if (!sheet || !rowUpdates || rowUpdates.length === 0) return;

  var normalizedUpdates = rowUpdates
    .map(function(item) {
      return {
        rowIndex: Number(item && item.rowIndex) || 0,
        values: (item && item.values) ? item.values.slice() : []
      };
    })
    .filter(function(item) {
      return item.rowIndex > 1 && item.values && item.values.length > 0;
    })
    .sort(function(a, b) {
      return a.rowIndex - b.rowIndex;
    });

  if (normalizedUpdates.length === 0) return;

  var groupStart = normalizedUpdates[0].rowIndex;
  var groupValues = [normalizedUpdates[0].values];
  var previousRowIndex = normalizedUpdates[0].rowIndex;
  var previousWidth = normalizedUpdates[0].values.length;

  function flushGroup_() {
    if (!groupValues.length) return;
    sheet.getRange(groupStart, 1, groupValues.length, groupValues[0].length).setValues(groupValues);
  }

  for (var i = 1; i < normalizedUpdates.length; i++) {
    var update = normalizedUpdates[i];
    var sameBlock = update.rowIndex === previousRowIndex + 1 && update.values.length === previousWidth;
    if (!sameBlock) {
      flushGroup_();
      groupStart = update.rowIndex;
      groupValues = [update.values];
    } else {
      groupValues.push(update.values);
    }
    previousRowIndex = update.rowIndex;
    previousWidth = update.values.length;
  }

  flushGroup_();
}

function writeSheetPartialRowsBatchByIndexes_(sheet, startColumn, width, rowUpdates) {
  if (!sheet || !rowUpdates || rowUpdates.length === 0) return;

  var normalizedStartColumn = Math.max(1, Number(startColumn) || 1);
  var normalizedWidth = Math.max(1, Number(width) || 1);

  var normalizedUpdates = rowUpdates
    .map(function(item) {
      var values = (item && item.values) ? item.values.slice(0, normalizedWidth) : [];
      while (values.length < normalizedWidth) values.push('');
      return {
        rowIndex: Number(item && item.rowIndex) || 0,
        values: values
      };
    })
    .filter(function(item) {
      return item.rowIndex > 1 && item.values && item.values.length === normalizedWidth;
    })
    .sort(function(a, b) {
      return a.rowIndex - b.rowIndex;
    });

  if (normalizedUpdates.length === 0) return;

  var groupStart = normalizedUpdates[0].rowIndex;
  var groupValues = [normalizedUpdates[0].values];
  var previousRowIndex = normalizedUpdates[0].rowIndex;

  function flushGroup_() {
    if (!groupValues.length) return;
    sheet.getRange(groupStart, normalizedStartColumn, groupValues.length, normalizedWidth).setValues(groupValues);
  }

  for (var i = 1; i < normalizedUpdates.length; i++) {
    var update = normalizedUpdates[i];
    var sameBlock = update.rowIndex === previousRowIndex + 1;
    if (!sameBlock) {
      flushGroup_();
      groupStart = update.rowIndex;
      groupValues = [update.values];
    } else {
      groupValues.push(update.values);
    }
    previousRowIndex = update.rowIndex;
  }

  flushGroup_();
}

function removeManagedPaymentByMatchedTransaction_(ss, matchedTx, actionLabel, successMessage) {
  if (!ss || !matchedTx) {
    throw new Error('ไม่พบข้อมูลรายการรับชำระที่ต้องการลบ');
  }

  var txId = String((matchedTx.values || [])[0] || '').trim();
  var contractNo = String((matchedTx.values || [])[2] || '').trim();
  if (!txId) throw new Error('ไม่พบรหัสอ้างอิงของรายการรับชำระ');
  if (!contractNo) throw new Error('ไม่พบเลขที่สัญญาของรายการรับชำระ');

  var plan = buildManagedContractRecalculationPlan_(ss, contractNo, {
    targetTxId: txId,
    targetMode: 'remove',
    replacementMatch: matchedTx
  });

  persistManagedContractRecalculationPlan_(ss, plan, [{
    sheetName: String(matchedTx.sheet && matchedTx.sheet.getName ? matchedTx.sheet.getName() || '' : ''),
    rowIndex: matchedTx.rowIndex
  }]);

  appendAuditLogSafe_(ss, {
    action: 'REMOVE_MANAGED_PAYMENT',
    entityType: 'TRANSACTION',
    entityId: txId,
    referenceNo: txId,
    before: buildAuditTransactionSnapshotFromValues_(matchedTx.values),
    reason: String(actionLabel || 'ลบรายการรับชำระย้อนหลัง'),
    details: 'ลบรายการรับชำระย้อนหลังและคำนวณยอดใหม่สำเร็จ'
  });
  logSystemActionFast(
    ss,
    actionLabel || 'ลบรายการรับชำระย้อนหลัง',
    'Transaction ID: ' + txId + ' | Contract: ' + contractNo + ' | Sheet: ' + String(matchedTx.sheet && matchedTx.sheet.getName ? matchedTx.sheet.getName() || '' : '')
  );
  syncTransactionIndexEntries_(ss, [{
    txId: txId
  }]);
  syncPaymentSearchIndexForContracts_(ss, [contractNo]);
  syncPaymentTodayIndexEntries_(ss, [{
    txId: txId
  }]);
  clearAppCache_();
  syncTransactionIndexVersionToCache_();
  syncPaymentSearchIndexVersionToCache_();
  syncPaymentTodayIndexVersionToCache_();

  return {
    status: 'Success',
    message: successMessage || 'กลับรายการรับชำระเรียบร้อยแล้ว',
    transactionId: txId,
    contractNo: contractNo
  };
}

function syncTransactionIndexEntries_(ss, entries) {
  var spreadsheet = ss || getDatabaseSpreadsheet_();
  var normalizedEntries = (entries || []).map(function(entry) {
    var rowValues = entry && entry.rowValues ? entry.rowValues.slice() : null;
    if (rowValues) {
      while (rowValues.length < 16) rowValues.push('');
    }

    var sourceSheet = entry && entry.sourceSheet ? entry.sourceSheet : null;
    var sourceSheetName = String(
      (entry && entry.sourceSheetName)
      || (sourceSheet && sourceSheet.getName ? sourceSheet.getName() : '')
      || ''
    ).trim();

    if (!sourceSheet && sourceSheetName) {
      sourceSheet = spreadsheet.getSheetByName(sourceSheetName);
    }

    return {
      txId: String((entry && entry.txId) || (rowValues && rowValues[0]) || '').trim(),
      rowValues: rowValues,
      rowIndex: Number(entry && entry.rowIndex) || 0,
      sourceSheet: sourceSheet,
      sourceSheetName: sourceSheetName
    };
  }).filter(function(entry) {
    return !!entry.txId;
  });

  if (!normalizedEntries.length) return null;

  var indexSheet = getTransactionIndexSheet_(spreadsheet);
  var rowsToDeleteMap = {};
  var rowsToAppend = [];

  for (var i = 0; i < normalizedEntries.length; i++) {
    var entry = normalizedEntries[i];
    var matchedRows = findExactRowsByTextFinderInColumn_(indexSheet, 1, entry.txId, 2);
    for (var d = 0; d < matchedRows.length; d++) {
      rowsToDeleteMap[matchedRows[d]] = true;
    }

    if (!entry.rowValues || !entry.sourceSheet || entry.rowIndex <= 1) continue;
    if (isInactiveTransactionStatus_(String(entry.rowValues[10] || '').trim())) continue;

    rowsToAppend.push(buildTransactionIndexRowFromValues_(entry.sourceSheet, entry.rowIndex, entry.rowValues));
  }

  var rowsToDelete = Object.keys(rowsToDeleteMap).map(function(key) {
    return Number(key) || 0;
  }).filter(function(rowIndex) {
    return rowIndex > 1;
  }).sort(function(a, b) {
    return a - b;
  });

  if (rowsToDelete.length) {
    deleteRowsByIndexes_(indexSheet, rowsToDelete);
  }
  if (rowsToAppend.length) {
    appendRowsSafely_(indexSheet, rowsToAppend, 'อัปเดตดัชนีธุรกรรม');
  }

  setPlainTextColumnFormat_(indexSheet, [1, 3, 4, 5, 6, 10, 11, 15]);
  indexSheet.hideSheet();
  invalidateSheetExecutionCaches_(indexSheet);
  syncTransactionIndexVersionToCache_();
  return indexSheet;
}

function upsertTransactionIndexEntry_(ss, sourceSheet, rowIndex, rowValues) {
  if (!ss || !sourceSheet || !rowIndex || !rowValues) return;

  syncTransactionIndexEntries_(ss, [{
    txId: String((rowValues || [])[0] || '').trim(),
    rowValues: rowValues,
    rowIndex: rowIndex,
    sourceSheet: sourceSheet
  }]);
}

function getCurrentTransactionSheetNameSet_(ss) {
  var spreadsheet = ss || getDatabaseSpreadsheet_();
  var set = {};
  var sheets = listCurrentTransactionSheets_(spreadsheet);
  for (var i = 0; i < sheets.length; i++) {
    var name = String(sheets[i] && sheets[i].getName ? sheets[i].getName() || '' : '').trim();
    if (name) set[name] = true;
  }
  return set;
}

function getTransactionIndexSheet_(ss) {
  var spreadsheet = ss || getDatabaseSpreadsheet_();
  var sheet = spreadsheet.getSheetByName(TRANSACTION_INDEX_SHEET_NAME)
    || createSheetSafely_(spreadsheet, TRANSACTION_INDEX_SHEET_NAME, 2, TRANSACTION_INDEX_HEADERS.length, 'เตรียมชีตดัชนีธุรกรรม');

  ensureSheetGridSizeSafely_(sheet, Math.max(sheet.getMaxRows(), 2), TRANSACTION_INDEX_HEADERS.length, 'เตรียมชีตดัชนีธุรกรรม');

  sheet.getRange(1, 1, 1, TRANSACTION_INDEX_HEADERS.length)
    .setValues([TRANSACTION_INDEX_HEADERS])
    .setFontWeight('bold')
    .setBackground('#ede9fe');
  if (sheet.getFrozenRows() < 1) sheet.setFrozenRows(1);
  sheet.hideSheet();
  return sheet;
}

function buildTransactionIndexRowFromValues_(sheet, rowIndex, rowValues) {
  var safeValues = (rowValues || []).slice();
  while (safeValues.length < 16) safeValues.push('');

  var timestamp = normalizeThaiDateCellValue_(safeValues[1], true);
  var parsed = parseThaiDateParts_(timestamp);
  var yearBe = parsed ? (Number(parsed.yearBe) || 0) : 0;
  var sortKey = buildTransactionOrderKey_(['', timestamp], rowIndex || 0);

  return [
    String(safeValues[0] || '').trim(),
    yearBe,
    timestamp,
    String(safeValues[2] || '').trim(),
    String(safeValues[3] || '').trim(),
    String(safeValues[9] || '').trim(),
    Number(safeValues[4]) || 0,
    Number(safeValues[5]) || 0,
    Number(safeValues[6]) || 0,
    String(safeValues[7] || '').trim(),
    String(sheet && sheet.getName ? sheet.getName() || '' : ''),
    Number(rowIndex) || 0,
    String(safeValues[10] || '').trim() || 'ปกติ',
    Math.max(0, Number(safeValues[11]) || 0),
    sortKey,
    Math.max(0, Number(safeValues[12]) || 0),
    Math.max(0, Number(safeValues[13]) || 0)
  ];
}

function rebuildTransactionIndexSheet_(ss) {
  var spreadsheet = ss || getDatabaseSpreadsheet_();
  var indexSheet = getTransactionIndexSheet_(spreadsheet);
  invalidateSheetExecutionCaches_(indexSheet);
  if (indexSheet.getLastRow() > 1) {
    indexSheet.getRange(2, 1, indexSheet.getLastRow() - 1, Math.max(indexSheet.getLastColumn(), TRANSACTION_INDEX_HEADERS.length)).clearContent();
  }

  var allSheets = listCurrentTransactionSheets_(spreadsheet).concat(listAllTransactionArchiveSheets_(spreadsheet));
  var rowsToWrite = [];
  for (var i = 0; i < allSheets.length; i++) {
    var sheet = allSheets[i];
    if (!sheet || sheet.getLastRow() <= 1) continue;

    ensureTransactionsSheetSchema_(sheet);
    var readCols = Math.min(Math.max(sheet.getLastColumn(), 14), 16);
    var values = sheet.getRange(2, 1, sheet.getLastRow() - 1, readCols).getValues();
    for (var r = 0; r < values.length; r++) {
      var row = values[r];
      if (!String(row[0] || '').trim()) continue;
      rowsToWrite.push(buildTransactionIndexRowFromValues_(sheet, r + 2, row));
    }
  }

  var chunkSize = 5000;
  if (rowsToWrite.length > 0) {
    for (var start = 0; start < rowsToWrite.length; start += chunkSize) {
      var chunk = rowsToWrite.slice(start, start + chunkSize);
      indexSheet.getRange(2 + start, 1, chunk.length, TRANSACTION_INDEX_HEADERS.length).setValues(chunk);
    }
  }

  setPlainTextColumnFormat_(indexSheet, [1, 3, 4, 5, 6, 10, 11, 15]);
  indexSheet.hideSheet();
  invalidateSheetExecutionCaches_(indexSheet);
  setTransactionIndexVersion_(getAppCacheVersion_());
  return indexSheet;
}

function ensureFreshTransactionIndexSheet_(ss, forceRebuild) {
  var spreadsheet = ss || getDatabaseSpreadsheet_();
  var desiredVersion = getAppCacheVersion_();
  var indexSheet = getTransactionIndexSheet_(spreadsheet);
  var currentVersion = getTransactionIndexVersion_();

  if (!forceRebuild && currentVersion === desiredVersion && indexSheet.getLastRow() > 1) {
    return indexSheet;
  }

  var lock = LockService.getScriptLock();
  var lockAcquired = false;
  try {
    lock.waitLock(30000);
    lockAcquired = true;
    currentVersion = getTransactionIndexVersion_();
    indexSheet = getTransactionIndexSheet_(spreadsheet);
    if (!forceRebuild && currentVersion === desiredVersion && indexSheet.getLastRow() > 1) {
      return indexSheet;
    }
    return rebuildTransactionIndexSheet_(spreadsheet);
  } finally {
    if (lockAcquired) lock.releaseLock();
  }
}

function buildManagedPaymentRecordFromIndexValues_(rowValues) {
  var row = (rowValues || []).slice();
  while (row.length < TRANSACTION_INDEX_HEADERS.length) row.push('');
  return {
    id: String(row[0] || '').trim(),
    timestamp: String(row[2] || '').trim(),
    contract: String(row[3] || '').trim(),
    memberId: String(row[4] || '').trim(),
    memberName: String(row[5] || '').trim(),
    principalPaid: Number(row[6]) || 0,
    interestPaid: Number(row[7]) || 0,
    newBalance: Number(row[8]) || 0,
    note: String(row[9] || '').trim(),
    sourceSheet: String(row[10] || '').trim(),
    rowIndex: Number(row[11]) || 0,
    interestMonthsPaid: Math.max(0, Number(row[13]) || 0),
    dateOnly: String(String(row[2] || '').trim()).split(' ')[0],
    dateNumber: getThaiDateNumber_(String(String(row[2] || '').trim()).split(' ')[0]),
    txStatus: String(row[12] || '').trim() || 'ปกติ',
    sortKey: String(row[14] || '').trim(),
    missedBeforePayment: Math.max(0, Number(row[15]) || 0),
    missedAfterPayment: Math.max(0, Number(row[16]) || 0)
  };
}

function getIndexedTransactionMatchById_(ss, txId) {
  if (!ss || !txId) return null;
  var indexSheet = ensureFreshTransactionIndexSheet_(ss);
  if (!indexSheet || indexSheet.getLastRow() <= 1) return null;

  var matchedRows = findRowsByExactValueInColumn_(indexSheet, 1, String(txId), 2);
  if (!matchedRows.length) return null;

  var indexRowMeta = readSheetRowsByIndexes_(indexSheet, [matchedRows[0]], TRANSACTION_INDEX_HEADERS.length);
  if (!indexRowMeta.length) return null;

  var indexValues = indexRowMeta[0].values;
  var sourceSheetName = String(indexValues[10] || '').trim();
  var sourceRowIndex = Number(indexValues[11]) || 0;
  if (!sourceSheetName || sourceRowIndex <= 1) return null;

  var sourceSheet = ss.getSheetByName(sourceSheetName);
  if (!sourceSheet || sourceSheet.getLastRow() < sourceRowIndex) return null;

  var readCols = Math.min(Math.max(sourceSheet.getLastColumn(), 14), 16);
  var sourceValues = sourceSheet.getRange(sourceRowIndex, 1, 1, readCols).getValues()[0];
  if (String(sourceValues[0] || '').trim() !== String(txId)) {
    return null;
  }

  return {
    sheet: sourceSheet,
    rowIndex: sourceRowIndex,
    values: sourceValues
  };
}

function getIndexedContractTransactionRows_(ss, contractNo) {
  var spreadsheet = ss || getDatabaseSpreadsheet_();
  var normalizedContract = String(contractNo || '').trim();
  if (!normalizedContract) return [];

  var indexSheet = ensureFreshTransactionIndexSheet_(spreadsheet);
  if (!indexSheet || indexSheet.getLastRow() <= 1) return [];

  var matchedRows = findRowsByExactValueInColumn_(indexSheet, 4, normalizedContract, 2);
  if (!matchedRows.length) return [];

  var indexRows = readSheetRowsByIndexes_(indexSheet, matchedRows, TRANSACTION_INDEX_HEADERS.length);
  var rowsBySheet = {};
  for (var i = 0; i < indexRows.length; i++) {
    var values = indexRows[i].values;
    var txStatus = String(values[12] || '').trim();
    if (isInactiveTransactionStatus_(txStatus)) continue;

    var sourceSheetName = String(values[10] || '').trim();
    var sourceRowIndex = Number(values[11]) || 0;
    if (!sourceSheetName || sourceRowIndex <= 1) continue;

    if (!rowsBySheet[sourceSheetName]) rowsBySheet[sourceSheetName] = [];
    rowsBySheet[sourceSheetName].push(sourceRowIndex);
  }

  var result = [];
  var sheetNames = Object.keys(rowsBySheet);
  for (var s = 0; s < sheetNames.length; s++) {
    var sheetName = sheetNames[s];
    var sheet = spreadsheet.getSheetByName(sheetName);
    if (!sheet) continue;
    ensureTransactionsSheetSchema_(sheet);
    var readCols = Math.min(Math.max(sheet.getLastColumn(), 14), 16);
    var rowMetas = readSheetRowsByIndexes_(sheet, rowsBySheet[sheetName], readCols);
    for (var r = 0; r < rowMetas.length; r++) {
      var rowMeta = rowMetas[r];
      if (String((rowMeta.values || [])[2] || '').trim() !== normalizedContract) continue;
      result.push({
        sheet: sheet,
        rowIndex: rowMeta.rowIndex,
        values: rowMeta.values
      });
    }
  }

  return result;
}


function buildIndexedTransactionRecordWithYear_(rowValues) {
  var row = (rowValues || []).slice();
  var record = buildManagedPaymentRecordFromIndexValues_(row);
  record.yearBe = Number(row[1]) || ((parseThaiDateParts_(record.dateOnly) || {}).yearBe || 0);
  return record;
}

function getIndexedContractTransactionRecords_(ss, contractNo) {
  var spreadsheet = ss || getDatabaseSpreadsheet_();
  var normalizedContract = String(contractNo || '').trim();
  if (!normalizedContract) return [];

  var indexSheet = ensureFreshTransactionIndexSheet_(spreadsheet);
  if (!indexSheet || indexSheet.getLastRow() <= 1) return [];

  var matchedRows = findRowsByExactValueInColumn_(indexSheet, 4, normalizedContract, 2);
  if (!matchedRows.length) return [];

  var rowMetas = readSheetRowsByIndexes_(indexSheet, matchedRows, TRANSACTION_INDEX_HEADERS.length);
  var records = [];
  for (var i = 0; i < rowMetas.length; i++) {
    var record = buildIndexedTransactionRecordWithYear_(rowMetas[i].values);
    if (isInactiveTransactionStatus_(record.txStatus)) continue;
    if (record.contract !== normalizedContract) continue;
    records.push(record);
  }

  return records;
}

function getIndexedMemberTransactionRecords_(ss, memberId) {
  var spreadsheet = ss || getDatabaseSpreadsheet_();
  var normalizedMemberId = normalizeTextForMatch_(memberId);
  if (!normalizedMemberId) return [];

  var indexSheet = ensureFreshTransactionIndexSheet_(spreadsheet);
  if (!indexSheet || indexSheet.getLastRow() <= 1) return [];

  var matchedRows = findRowsByExactValueInColumn_(indexSheet, 5, normalizedMemberId, 2);
  if (!matchedRows.length) return [];

  var rowMetas = readSheetRowsByIndexes_(indexSheet, matchedRows, TRANSACTION_INDEX_HEADERS.length);
  var records = [];
  for (var i = 0; i < rowMetas.length; i++) {
    var record = buildIndexedTransactionRecordWithYear_(rowMetas[i].values);
    if (isInactiveTransactionStatus_(record.txStatus)) continue;
    if (normalizeTextForMatch_(record.memberId) !== normalizedMemberId) continue;
    records.push(record);
  }

  return records;
}

function getActiveIndexedTransactionRecordsCached_(ss, minYearBe) {
  var spreadsheet = ss || getDatabaseSpreadsheet_();
  var indexSheet = ensureFreshTransactionIndexSheet_(spreadsheet);
  if (!indexSheet || indexSheet.getLastRow() <= 1) return [];

  var normalizedMinYearBe = Number(minYearBe) || 0;
  var cacheKey = getSheetExecutionCacheKey_(indexSheet, -TRANSACTION_INDEX_HEADERS.length, 2) + '::activeIndexed::' + normalizedMinYearBe;
  if (EXECUTION_ACTIVE_TRANSACTION_RECORDS_CACHE_.hasOwnProperty(cacheKey)) {
    return EXECUTION_ACTIVE_TRANSACTION_RECORDS_CACHE_[cacheKey];
  }

  var rows = indexSheet.getRange(2, 1, indexSheet.getLastRow() - 1, TRANSACTION_INDEX_HEADERS.length).getValues();
  var records = [];
  for (var i = 0; i < rows.length; i++) {
    var record = buildIndexedTransactionRecordWithYear_(rows[i]);
    if (isInactiveTransactionStatus_(record.txStatus)) continue;
    if (normalizedMinYearBe && Number(record.yearBe || 0) && Number(record.yearBe || 0) < normalizedMinYearBe) continue;
    records.push(record);
  }

  EXECUTION_ACTIVE_TRANSACTION_RECORDS_CACHE_[cacheKey] = records;
  return records;
}

function normalizeTextForMatch_(value) {
  return String(value || "").replace(/\s+/g, " ").trim();
}

function validateRequiredTextField_(value, label) {
  var text = normalizeTextForMatch_(value);
  if (!text) {
    throw new Error('กรุณาระบุ' + label);
  }
  return text;
}

function validateLoanPayloadForWrite_(loanData) {
  var payload = loanData || {};
  var normalized = {
    memberId: validateRequiredTextField_(payload.memberId, 'รหัสสมาชิก'),
    contract: validateRequiredTextField_(payload.contract, 'เลขที่สัญญา'),
    member: validateRequiredTextField_(payload.member, 'ชื่อ-สกุลสมาชิก'),
    amount: Number(payload.amount),
    interest: Number(payload.interest),
    balance: Number(payload.balance),
    status: normalizeTextForMatch_(payload.status) || 'ปกติ',
    nextPayment: String(payload.nextPayment || '-').trim() || '-',
    missedInterestMonths: Math.max(0, Number(payload.missedInterestMonths) || 0),
    createdAt: payload.createdAt,
    guarantor1: String(payload.guarantor1 || ''),
    guarantor2: String(payload.guarantor2 || '')
  };

  if (!isFinite(normalized.amount) || normalized.amount <= 0) {
    throw new Error('ยอดเงินกู้ต้องมากกว่า 0');
  }
  if (!isFinite(normalized.interest) || normalized.interest < 0) {
    throw new Error('อัตราดอกเบี้ยต้องเป็นตัวเลขตั้งแต่ 0 ขึ้นไป');
  }
  if (!isFinite(normalized.balance) || normalized.balance < 0) {
    throw new Error('ยอดคงเหลือต้องเป็นตัวเลขตั้งแต่ 0 ขึ้นไป');
  }
  if (normalized.balance - normalized.amount > 0.0001) {
    throw new Error('ยอดคงเหลือห้ามมากกว่ายอดเงินกู้');
  }

  return normalized;
}

function validateMemberPayloadForWrite_(memberData) {
  var payload = memberData || {};
  return {
    id: validateRequiredTextField_(payload.id, 'รหัสสมาชิก'),
    name: validateRequiredTextField_(payload.name, 'ชื่อ-สกุลสมาชิก'),
    status: normalizeTextForMatch_(payload.status) || 'ปกติ'
  };
}

function hasActiveTransactionForMember_(ss, memberId) {
  var spreadsheet = ss || getDatabaseSpreadsheet_();
  var normalizedMemberId = String(memberId || '').trim();
  if (!normalizedMemberId) return false;

  var indexSheet = ensureFreshTransactionIndexSheet_(spreadsheet);
  if (!indexSheet || indexSheet.getLastRow() <= 1) return false;

  var matchedRows = findRowsByExactValueInColumn_(indexSheet, 5, normalizedMemberId, 2);
  if (!matchedRows.length) return false;

  var rowMetas = readSheetRowsByIndexes_(indexSheet, matchedRows, TRANSACTION_INDEX_HEADERS.length);
  for (var i = 0; i < rowMetas.length; i++) {
    var values = rowMetas[i].values || [];
    if (isInactiveTransactionStatus_(String(values[12] || '').trim())) continue;
    return true;
  }
  return false;
}

function formatMemberDisplayName_(memberId, memberName) {
  var id = normalizeTextForMatch_(memberId);
  var name = normalizeTextForMatch_(memberName);

  if (id && name) return "(" + id + ") " + name;
  return name || id;
}

function extractMemberIdFromGuarantorText_(value) {
  var text = normalizeTextForMatch_(value);
  if (!text) return "";

  var parenMatch = text.match(/^\(([^)]+)\)\s*(.*)$/);
  if (parenMatch) {
    return normalizeTextForMatch_(parenMatch[1]);
  }

  var parts = text.split(" ");
  if (parts.length >= 2) {
    var firstToken = normalizeTextForMatch_(parts[0]);
    if (/^(?:[A-Za-z]+[-\\/]?)?\d[\w\\/-]*$/i.test(firstToken)) {
      return firstToken;
    }
  }

  return "";
}

function guarantorReferenceMatchesMember_(guarantorText, memberId, memberName) {
  var text = normalizeTextForMatch_(guarantorText);
  var id = normalizeTextForMatch_(memberId);
  var name = normalizeTextForMatch_(memberName);

  if (!text) return false;
  if (id && text === id) return true;
  if (name && text === name) return true;

  var extractedId = extractMemberIdFromGuarantorText_(text);
  if (id && extractedId && extractedId === id) return true;

  return false;
}

function syncGuarantorReferencesForMember_(ss, memberId, previousMemberName, updatedMemberName) {
  var loanSheet = ss.getSheetByName("Loans");
  if (!loanSheet || loanSheet.getLastRow() <= 1) return 0;

  var memberIdText = normalizeTextForMatch_(memberId);
  var oldNameText = normalizeTextForMatch_(previousMemberName);
  var newNameText = normalizeTextForMatch_(updatedMemberName);
  if (!memberIdText || !newNameText) return 0;

  var formattedGuarantorText = formatMemberDisplayName_(memberIdText, newNameText);
  var lastRow = loanSheet.getLastRow();
  var loanRange = loanSheet.getRange(2, 1, lastRow - 1, Math.max(loanSheet.getLastColumn(), 12));
  var loanData = loanRange.getValues();
  var updatedCount = 0;

  for (var i = 0; i < loanData.length; i++) {
    var rowChanged = false;

    for (var colIndex = 10; colIndex <= 11; colIndex++) {
      var currentValue = loanData[i][colIndex];
      if (
        guarantorReferenceMatchesMember_(currentValue, memberIdText, oldNameText) ||
        guarantorReferenceMatchesMember_(currentValue, memberIdText, newNameText)
      ) {
        if (normalizeTextForMatch_(currentValue) !== formattedGuarantorText) {
          loanData[i][colIndex] = formattedGuarantorText;
          rowChanged = true;
        }
      }
    }

    if (rowChanged) updatedCount++;
  }

  if (updatedCount > 0) {
    loanRange.setValues(loanData);
  }

  return updatedCount;
}


var TEMP_GUARANTOR_STATUS = "ผู้ค้ำชั่วคราว";
var TEMP_GUARANTOR_ID_PREFIX = "TMP-";

function isTemporaryGuarantorStatus_(status) {
  return normalizeTextForMatch_(status) === TEMP_GUARANTOR_STATUS;
}

function normalizeMemberNameKey_(value) {
  return String(value || "").replace(/\s+/g, " ").trim().toLowerCase();
}

function parseGuarantorInputParts_(value) {
  var rawText = normalizeTextForMatch_(value);
  if (!rawText) {
    return { rawText: "", memberId: "", memberName: "" };
  }

  var parenMatch = rawText.match(/^\(([^)]+)\)\s*(.*)$/);
  if (parenMatch) {
    return {
      rawText: rawText,
      memberId: normalizeTextForMatch_(parenMatch[1]),
      memberName: normalizeTextForMatch_(parenMatch[2]) || rawText
    };
  }

  var tokenMatch = rawText.match(/^([A-Za-z]+[-\\/]?\d[\w\\/-]*)\s+(.+)$/i);
  if (tokenMatch) {
    return {
      rawText: rawText,
      memberId: normalizeTextForMatch_(tokenMatch[1]),
      memberName: normalizeTextForMatch_(tokenMatch[2])
    };
  }

  return {
    rawText: rawText,
    memberId: "",
    memberName: rawText
  };
}

function registerMemberIndexEntry_(memberIndex, entry) {
  if (!memberIndex || !entry) return;

  var memberId = normalizeTextForMatch_(entry.id);
  var memberName = normalizeTextForMatch_(entry.name);
  var status = normalizeTextForMatch_(entry.status);

  if (!memberId && !memberName) return;

  var normalizedEntry = {
    id: memberId,
    name: memberName,
    status: status,
    row: entry.row || 0
  };

  if (memberId) {
    memberIndex.byId[memberId] = normalizedEntry;
  }

  var nameKey = normalizeMemberNameKey_(memberName);
  if (nameKey) {
    if (!memberIndex.byNameKey[nameKey]) memberIndex.byNameKey[nameKey] = [];
    var existingIndex = -1;
    for (var i = 0; i < memberIndex.byNameKey[nameKey].length; i++) {
      if (memberIndex.byNameKey[nameKey][i].id === memberId && memberId) {
        existingIndex = i;
        break;
      }
    }
    if (existingIndex >= 0) {
      memberIndex.byNameKey[nameKey][existingIndex] = normalizedEntry;
    } else {
      memberIndex.byNameKey[nameKey].push(normalizedEntry);
    }
  }
}

function buildMemberRegistryIndex_(memberSheet) {
  var result = { byId: {}, byNameKey: {} };
  if (!memberSheet || memberSheet.getLastRow() <= 1) return result;

  var lastRow = memberSheet.getLastRow();
  var lastCol = Math.max(memberSheet.getLastColumn(), 3);
  var memberData = memberSheet.getRange(2, 1, lastRow - 1, lastCol).getValues();

  for (var i = 0; i < memberData.length; i++) {
    registerMemberIndexEntry_(result, {
      id: memberData[i][0],
      name: memberData[i][1],
      status: memberData[i][2],
      row: i + 2
    });
  }

  return result;
}

function pickSingleMemberCandidate_(candidates) {
  if (!candidates || candidates.length === 0) return null;
  if (candidates.length === 1) return candidates[0];

  var realMembers = [];
  var tempMembers = [];
  for (var i = 0; i < candidates.length; i++) {
    if (isTemporaryGuarantorStatus_(candidates[i].status)) {
      tempMembers.push(candidates[i]);
    } else {
      realMembers.push(candidates[i]);
    }
  }

  if (realMembers.length === 1) return realMembers[0];
  if (realMembers.length === 0 && tempMembers.length === 1) return tempMembers[0];

  return null;
}

function findBestMemberMatchForGuarantor_(memberIndex, guarantorText) {
  if (!memberIndex) return null;

  var rawText = normalizeTextForMatch_(guarantorText);
  if (!rawText) return null;

  var parsed = parseGuarantorInputParts_(rawText);
  var explicitId = normalizeTextForMatch_(parsed.memberId);
  if (explicitId && memberIndex.byId[explicitId]) {
    return memberIndex.byId[explicitId];
  }

  if (memberIndex.byId[rawText]) {
    return memberIndex.byId[rawText];
  }

  var nameKey = normalizeMemberNameKey_(parsed.memberName || rawText);
  if (nameKey) {
    return pickSingleMemberCandidate_(memberIndex.byNameKey[nameKey] || []);
  }

  return null;
}

function generateNextTemporaryMemberId_(memberSheet) {
  var maxSeq = 0;
  if (memberSheet && memberSheet.getLastRow() > 1) {
    var ids = memberSheet.getRange(2, 1, memberSheet.getLastRow() - 1, 1).getValues();
    for (var i = 0; i < ids.length; i++) {
      var text = normalizeTextForMatch_(ids[i][0]);
      var match = text.match(/^TMP-(\d+)$/i);
      if (match) {
        maxSeq = Math.max(maxSeq, Number(match[1]) || 0);
      }
    }
  }
  return TEMP_GUARANTOR_ID_PREFIX + ("00000" + (maxSeq + 1)).slice(-5);
}

function createTemporaryGuarantorMember_(memberSheet, memberIndex, guarantorName) {
  var cleanName = normalizeTextForMatch_(guarantorName);
  if (!cleanName) return null;

  var tempId = generateNextTemporaryMemberId_(memberSheet);
  var targetRow = appendRowsSafely_(memberSheet, [[tempId, cleanName, TEMP_GUARANTOR_STATUS]], 'เพิ่มผู้ค้ำชั่วคราว');

  var entry = {
    id: tempId,
    name: cleanName,
    status: TEMP_GUARANTOR_STATUS,
    row: targetRow
  };
  registerMemberIndexEntry_(memberIndex, entry);
  return entry;
}

function ensureGuarantorMemberRecord_(memberSheet, memberIndex, guarantorText) {
  var rawText = normalizeTextForMatch_(guarantorText);
  if (!rawText) {
    return {
      memberId: "",
      memberName: "",
      formatted: "",
      createdTemporary: false,
      ambiguous: false,
      rawText: ""
    };
  }

  var directMatch = findBestMemberMatchForGuarantor_(memberIndex, rawText);
  if (directMatch) {
    return {
      memberId: directMatch.id,
      memberName: directMatch.name,
      formatted: formatMemberDisplayName_(directMatch.id, directMatch.name),
      createdTemporary: false,
      ambiguous: false,
      rawText: rawText
    };
  }

  var parsed = parseGuarantorInputParts_(rawText);
  var nameKey = normalizeMemberNameKey_(parsed.memberName || rawText);
  var sameNameCandidates = nameKey ? (memberIndex.byNameKey[nameKey] || []) : [];
  if (sameNameCandidates.length > 1 && !pickSingleMemberCandidate_(sameNameCandidates)) {
    return {
      memberId: "",
      memberName: normalizeTextForMatch_(parsed.memberName || rawText),
      formatted: rawText,
      createdTemporary: false,
      ambiguous: true,
      rawText: rawText
    };
  }

  var tempEntry = pickSingleMemberCandidate_(sameNameCandidates);
  if (!tempEntry) {
    tempEntry = createTemporaryGuarantorMember_(memberSheet, memberIndex, parsed.memberName || rawText);
  }

  if (!tempEntry) {
    return {
      memberId: "",
      memberName: normalizeTextForMatch_(parsed.memberName || rawText),
      formatted: rawText,
      createdTemporary: false,
      ambiguous: false,
      rawText: rawText
    };
  }

  return {
    memberId: tempEntry.id,
    memberName: tempEntry.name,
    formatted: formatMemberDisplayName_(tempEntry.id, tempEntry.name),
    createdTemporary: isTemporaryGuarantorStatus_(tempEntry.status),
    ambiguous: false,
    rawText: rawText
  };
}

function normalizeLoanGuarantorsForSave_(ss, guarantor1Value, guarantor2Value) {
  var memberSheet = getSheetOrThrow(ss, "Members");
  var memberIndex = buildMemberRegistryIndex_(memberSheet);

  var result1 = ensureGuarantorMemberRecord_(memberSheet, memberIndex, guarantor1Value);
  var result2 = ensureGuarantorMemberRecord_(memberSheet, memberIndex, guarantor2Value);

  return {
    guarantor1: result1.formatted || result1.rawText || "",
    guarantor2: result2.formatted || result2.rawText || "",
    createdTemporaryCount: (result1.createdTemporary ? 1 : 0) + (result2.createdTemporary ? 1 : 0),
    ambiguousCount: (result1.ambiguous ? 1 : 0) + (result2.ambiguous ? 1 : 0),
    shouldSortMembers: result1.createdTemporary || result2.createdTemporary
  };
}

function guarantorReferenceMatchesIdentity_(guarantorText, memberId, memberName) {
  var text = normalizeTextForMatch_(guarantorText);
  var id = normalizeTextForMatch_(memberId);
  var name = normalizeTextForMatch_(memberName);

  if (!text) return false;
  if (id && (text === id || extractMemberIdFromGuarantorText_(text) === id)) return true;
  if (name) {
    if (text === name) return true;
    var parsed = parseGuarantorInputParts_(text);
    if (normalizeMemberNameKey_(parsed.memberName || text) === normalizeMemberNameKey_(name)) {
      return true;
    }
  }

  return false;
}

function syncGuarantorIdentityChangeInLoans_(ss, oldMemberId, oldMemberName, newMemberId, newMemberName) {
  var loanSheet = ss.getSheetByName("Loans");
  if (!loanSheet || loanSheet.getLastRow() <= 1) return 0;

  var normalizedNewId = normalizeTextForMatch_(newMemberId);
  var normalizedNewName = normalizeTextForMatch_(newMemberName);
  if (!normalizedNewId || !normalizedNewName) return 0;

  var formattedValue = formatMemberDisplayName_(normalizedNewId, normalizedNewName);
  var loanRange = loanSheet.getRange(2, 1, loanSheet.getLastRow() - 1, Math.max(loanSheet.getLastColumn(), 12));
  var loanData = loanRange.getValues();
  var updatedRows = 0;

  for (var i = 0; i < loanData.length; i++) {
    var rowChanged = false;
    for (var colIndex = 10; colIndex <= 11; colIndex++) {
      var currentValue = loanData[i][colIndex];
      if (guarantorReferenceMatchesIdentity_(currentValue, oldMemberId, oldMemberName)) {
        if (normalizeTextForMatch_(currentValue) !== formattedValue) {
          loanData[i][colIndex] = formattedValue;
          rowChanged = true;
        }
      }
    }
    if (rowChanged) updatedRows++;
  }

  if (updatedRows > 0) {
    loanRange.setValues(loanData);
  }

  return updatedRows;
}

function upgradeTemporaryMemberToReal_(ss, memberId, memberName, finalStatus) {
  var memberSheet = getSheetOrThrow(ss, "Members");
  var memberIndex = buildMemberRegistryIndex_(memberSheet);
  var nameKey = normalizeMemberNameKey_(memberName);
  var candidates = nameKey ? (memberIndex.byNameKey[nameKey] || []) : [];
  var tempCandidates = [];
  for (var i = 0; i < candidates.length; i++) {
    if (isTemporaryGuarantorStatus_(candidates[i].status)) {
      tempCandidates.push(candidates[i]);
    }
  }

  if (tempCandidates.length !== 1) return null;

  var tempEntry = tempCandidates[0];
  memberSheet.getRange(tempEntry.row, 1, 1, 3).setValues([[
    memberId,
    memberName,
    finalStatus || "ปกติ"
  ]]);

  syncGuarantorIdentityChangeInLoans_(ss, tempEntry.id, tempEntry.name, memberId, memberName);
  sortMembersSheetByMemberId_(memberSheet);

  return {
    previousTempId: tempEntry.id,
    updatedMemberId: memberId,
    updatedMemberName: memberName
  };
}

function registerTemporaryGuarantorMembersFromLoans_() {
  var ss = getDatabaseSpreadsheet_();
  var loanSheet = getSheetOrThrow(ss, "Loans");
  var memberSheet = getSheetOrThrow(ss, "Members");

  if (loanSheet.getLastRow() <= 1) {
    return {
      status: "Success",
      createdMembers: 0,
      updatedLoanRows: 0,
      updatedGuarantorCells: 0,
      ambiguousEntries: 0,
      message: "ไม่มีข้อมูลสัญญาในชีต Loans"
    };
  }

  var memberIndex = buildMemberRegistryIndex_(memberSheet);
  var lastRow = loanSheet.getLastRow();
  var lastCol = Math.max(loanSheet.getLastColumn(), 12);
  var loanRange = loanSheet.getRange(2, 1, lastRow - 1, lastCol);
  var loanData = loanRange.getValues();

  var createdMembers = 0;
  var updatedLoanRows = 0;
  var updatedGuarantorCells = 0;
  var ambiguousEntries = 0;

  for (var i = 0; i < loanData.length; i++) {
    var rowChanged = false;
    for (var colIndex = 10; colIndex <= 11; colIndex++) {
      var currentValue = loanData[i][colIndex];
      if (!normalizeTextForMatch_(currentValue)) continue;

      var ensured = ensureGuarantorMemberRecord_(memberSheet, memberIndex, currentValue);
      if (ensured.createdTemporary) createdMembers++;
      if (ensured.ambiguous) ambiguousEntries++;

      if (ensured.formatted && normalizeTextForMatch_(currentValue) !== ensured.formatted) {
        loanData[i][colIndex] = ensured.formatted;
        rowChanged = true;
        updatedGuarantorCells++;
      }
    }

    if (rowChanged) updatedLoanRows++;
  }

  if (updatedLoanRows > 0) {
    loanRange.setValues(loanData);
  }

  sortLoansSheetByRules_(loanSheet);

  if (createdMembers > 0) {
    sortMembersSheetByMemberId_(memberSheet);
  }

  clearAppCache_();
  logSystemActionFast(
    ss,
    "นำผู้ค้ำเข้าทะเบียนสมาชิก",
    "สร้างผู้ค้ำชั่วคราว " + createdMembers + " รายการ | อัปเดตสัญญา " + updatedLoanRows + " รายการ | ผู้ค้ำกำกวม " + ambiguousEntries + " รายการ"
  );

  return {
    status: "Success",
    createdMembers: createdMembers,
    updatedLoanRows: updatedLoanRows,
    updatedGuarantorCells: updatedGuarantorCells,
    ambiguousEntries: ambiguousEntries,
    message: "สร้างผู้ค้ำชั่วคราวและอัปเดต Loans เรียบร้อยแล้ว"
  };
}

function registerTemporaryGuarantorMembersFromLoans() {
  requireAdminSession_();
  var lock = LockService.getScriptLock();
  var lockAcquired = false;

  try {
    lock.waitLock(30000);
    lockAcquired = true;
    return registerTemporaryGuarantorMembersFromLoans_();
  } catch (e) {
    return {
      status: "Error",
      message: "ไม่สามารถนำผู้ค้ำเข้าทะเบียนสมาชิกได้: " + e.toString()
    };
  } finally {
    if (lockAcquired) lock.releaseLock();
  }
}



function buildMemberLookupIndex_(memberSheet) {
  var result = { byId: {}, byName: {} };
  if (!memberSheet || memberSheet.getLastRow() <= 1) return result;

  var lastRow = memberSheet.getLastRow();
  var lastCol = Math.max(memberSheet.getLastColumn(), 2);
  var memberData = memberSheet.getRange(2, 1, lastRow - 1, lastCol).getValues();

  for (var i = 0; i < memberData.length; i++) {
    var memberId = normalizeTextForMatch_(memberData[i][0]);
    var memberName = normalizeTextForMatch_(memberData[i][1]);

    if (!memberId || !memberName) continue;

    var payload = {
      id: memberId,
      name: memberName,
      formatted: formatMemberDisplayName_(memberId, memberName)
    };

    result.byId[memberId] = payload;
    if (!result.byName[memberName]) {
      result.byName[memberName] = payload;
    }
  }

  return result;
}

function resolveMemberFromGuarantorText_(memberLookup, guarantorText) {
  var text = normalizeTextForMatch_(guarantorText);
  if (!text || !memberLookup) return null;

  var extractedId = extractMemberIdFromGuarantorText_(text);
  if (extractedId && memberLookup.byId[extractedId]) {
    return memberLookup.byId[extractedId];
  }

  if (memberLookup.byId[text]) {
    return memberLookup.byId[text];
  }

  if (memberLookup.byName[text]) {
    return memberLookup.byName[text];
  }

  return null;
}

function syncAllLoanMembersFromRegister_() {
  var ss = getDatabaseSpreadsheet_();
  var loanSheet = getSheetOrThrow(ss, "Loans");
  var memberSheet = getSheetOrThrow(ss, "Members");

  if (loanSheet.getLastRow() <= 1) {
    return {
      status: "Success",
      updatedLoanRows: 0,
      updatedBorrowers: 0,
      updatedGuarantors: 0,
      createdTemporaryMembers: 0,
      message: "ไม่มีข้อมูลสัญญาในชีต Loans"
    };
  }

  var memberIndex = buildMemberRegistryIndex_(memberSheet);
  var lastRow = loanSheet.getLastRow();
  var lastCol = Math.max(loanSheet.getLastColumn(), 12);
  var loanRange = loanSheet.getRange(2, 1, lastRow - 1, lastCol);
  var loanData = loanRange.getValues();

  var updatedLoanRows = 0;
  var updatedBorrowers = 0;
  var updatedGuarantors = 0;
  var createdTemporaryMembers = 0;

  for (var i = 0; i < loanData.length; i++) {
    var rowChanged = false;
    var memberId = normalizeTextForMatch_(loanData[i][0]);
    var borrower = memberId ? memberIndex.byId[memberId] : null;

    if (borrower) {
      var currentMemberName = normalizeTextForMatch_(loanData[i][2]);
      if (currentMemberName !== borrower.name) {
        loanData[i][2] = borrower.name;
        rowChanged = true;
        updatedBorrowers += 1;
      }
    }

    for (var colIndex = 10; colIndex <= 11; colIndex++) {
      var currentGuarantor = loanData[i][colIndex];
      if (!normalizeTextForMatch_(currentGuarantor)) continue;

      var ensured = ensureGuarantorMemberRecord_(memberSheet, memberIndex, currentGuarantor);
      if (ensured.createdTemporary) {
        createdTemporaryMembers += 1;
      }

      if (ensured.formatted && normalizeTextForMatch_(currentGuarantor) !== ensured.formatted) {
        loanData[i][colIndex] = ensured.formatted;
        rowChanged = true;
        updatedGuarantors += 1;
      }
    }

    if (rowChanged) {
      updatedLoanRows += 1;
    }
  }

  if (updatedLoanRows > 0) {
    loanRange.setValues(loanData);
  }

  sortLoansSheetByRules_(loanSheet);

  if (createdTemporaryMembers > 0) {
    sortMembersSheetByMemberId_(memberSheet);
  }

  clearAppCache_();
  logSystemActionFast(
    ss,
    "อัพเดตฐานข้อมูลสมาชิกใน Loans",
    "อัปเดต " + updatedLoanRows + " สัญญา | ชื่อผู้กู้ " + updatedBorrowers + " รายการ | ผู้ค้ำ " + updatedGuarantors + " รายการ | สร้างผู้ค้ำชั่วคราว " + createdTemporaryMembers + " รายการ"
  );

  return {
    status: "Success",
    updatedLoanRows: updatedLoanRows,
    updatedBorrowers: updatedBorrowers,
    updatedGuarantors: updatedGuarantors,
    createdTemporaryMembers: createdTemporaryMembers,
    message: "อัปเดตข้อมูลสมาชิกใน Loans เรียบร้อยแล้ว"
  };
}

function syncAllLoanMembersFromRegister() {
  requirePermission_('settings.manage', 'ไม่มีสิทธิ์อัพเดตฐานข้อมูลสมาชิก');
  var lock = LockService.getScriptLock();
  var lockAcquired = false;

  try {
    lock.waitLock(30000);
    lockAcquired = true;
    return syncAllLoanMembersFromRegister_();
  } catch (e) {
    return {
      status: "Error",
      message: "ไม่สามารถอัพเดตฐานข้อมูลสมาชิกได้: " + e.toString()
    };
  } finally {
    if (lockAcquired) lock.releaseLock();
  }
}


function getSheetOrThrow(ss, sheetName) {
  var sheet = ss.getSheetByName(sheetName);
  if (!sheet) throw new Error("ไม่พบชีต: " + sheetName);
  return sheet;
}

function shouldExcludeLoanFromArrearsCount_(balance, status) {
  var remainingBalance = Number(balance) || 0;
  var normalizedStatus = String(status || "").trim();
  if (remainingBalance <= 0) return true;
  if (normalizedStatus.indexOf("กลบหนี้") !== -1) return true;
  if (normalizedStatus.indexOf("ปิดบัญชี") !== -1) return true;
  return false;
}

function deriveLoanStatusFromState_(balance, missedMonths, fallbackStatus) {
  var remainingBalance = Number(balance) || 0;
  var arrears = Number(missedMonths) || 0;
  var fallback = String(fallbackStatus || "").trim();

  if (remainingBalance <= 0) {
    if (fallback && fallback.indexOf("กลบหนี้") !== -1) return fallback;
    return "ปิดบัญชี";
  }
  if (arrears > 0) return "ค้างชำระ";

  return fallback || "ปกติ";
}

function ensureTransactionsSheetSchema_(sheet) {
  if (!sheet) return;

  var requiredHeaders = REQUIRED_TRANSACTION_HEADERS_;
  if (sheet.getLastRow() === 0) {
    ensureSheetGridSizeSafely_(sheet, 2, requiredHeaders.length, 'เตรียม schema ชีตธุรกรรม');
    sheet.getRange(1, 1, 1, requiredHeaders.length)
      .setValues([requiredHeaders])
      .setFontWeight('bold')
      .setBackground('#fffbeb');
    setPlainTextColumnFormat_(sheet, [1, 2, 3, 4]);
    return;
  }

  var lastColumn = Math.max(sheet.getLastColumn(), requiredHeaders.length);
  ensureSheetGridSizeSafely_(sheet, Math.max(sheet.getMaxRows(), 2), lastColumn, 'เตรียม schema ชีตธุรกรรม');
  var headers = sheet.getRange(1, 1, 1, lastColumn).getValues()[0];
  var schemaChanged = false;

  for (var i = 0; i < requiredHeaders.length; i++) {
    if (String(headers[i] || '').trim() === requiredHeaders[i]) continue;
    if (headers.indexOf(requiredHeaders[i]) !== -1) continue;
    sheet.getRange(1, i + 1)
      .setValue(requiredHeaders[i])
      .setFontWeight('bold')
      .setBackground('#fffbeb');
    headers[i] = requiredHeaders[i];
    schemaChanged = true;
  }

  if (schemaChanged) {
    invalidateSheetExecutionCaches_(sheet);
  }
  setPlainTextColumnFormat_(sheet, [1, 2, 3, 4]);
}

function findRowByExactValue(sheet, columnIndex, value) {
  if (!sheet) return 0;
  var text = String(value || "").trim();
  if (!text) return 0;
  var matchedRows = getExactRowIndexesByColumnValueCached_(sheet, columnIndex, text, 2);
  return matchedRows.length ? matchedRows[0] : 0;
}

function syncLoanBorrowerNameForMember_(ss, memberId, updatedMemberName) {
  var loanSheet = ss.getSheetByName("Loans");
  if (!loanSheet || loanSheet.getLastRow() <= 1) return [];

  var normalizedMemberId = String(memberId || "").trim();
  var normalizedMemberName = String(updatedMemberName || "").trim();
  if (!normalizedMemberId || !normalizedMemberName) return [];

  var lastCol = Math.max(loanSheet.getLastColumn(), 12);
  var loanRange = loanSheet.getRange(2, 1, loanSheet.getLastRow() - 1, lastCol);
  var loanData = loanRange.getValues();
  var changedContracts = [];

  for (var i = 0; i < loanData.length; i++) {
    if (String(loanData[i][0] || "").trim() !== normalizedMemberId) continue;
    if (String(loanData[i][2] || "").trim() === normalizedMemberName) continue;
    loanData[i][2] = normalizedMemberName;
    changedContracts.push(String(loanData[i][1] || "").trim());
  }

  if (changedContracts.length > 0) {
    loanRange.setValues(loanData);
    sortLoansSheetByRules_(loanSheet);
  }

  return changedContracts;
}

function syncMemberRegisterFromLoanEdit_(ss, previousMemberId, updatedMemberId, updatedMemberName) {
  var memberSheet = ss.getSheetByName("Members");
  if (!memberSheet || memberSheet.getLastRow() <= 1) return;

  var nextMemberId = String(updatedMemberId || "").trim();
  var nextMemberName = String(updatedMemberName || "").trim();
  var oldMemberId = String(previousMemberId || "").trim();

  if (!nextMemberId || !nextMemberName) return;

  var memberRow = findRowByExactValue(memberSheet, 1, nextMemberId);
  if (!memberRow && oldMemberId && oldMemberId !== nextMemberId) {
    memberRow = findRowByExactValue(memberSheet, 1, oldMemberId);
  }
  if (!memberRow) return;

  var currentMemberValues = memberSheet.getRange(memberRow, 1, 1, 3).getValues()[0];
  var previousMemberName = String(currentMemberValues[1] || "").trim();
  var memberStatus = currentMemberValues[2];

  memberSheet.getRange(memberRow, 1, 1, 3).setValues([[
    nextMemberId,
    nextMemberName,
    memberStatus
  ]]);
  sortMembersSheetByMemberId_(memberSheet);
  syncGuarantorReferencesForMember_(ss, nextMemberId, previousMemberName, nextMemberName);
}

function sortMembersSheetByMemberId_(memberSheet) {
  if (!memberSheet || memberSheet.getLastRow() <= 2) return;

  var lastRow = memberSheet.getLastRow();
  var lastColumn = Math.max(memberSheet.getLastColumn(), 3);
  var dataRange = memberSheet.getRange(2, 1, lastRow - 1, lastColumn);
  var data = dataRange.getValues();

  data.sort(function(a, b) {
    var aId = String(a[0] || "").trim();
    var bId = String(b[0] || "").trim();

    var aNum = Number(aId);
    var bNum = Number(bId);
    var aIsNum = aId !== "" && !isNaN(aNum);
    var bIsNum = bId !== "" && !isNaN(bNum);

    if (aIsNum && bIsNum) {
      if (aNum !== bNum) return aNum - bNum;
      return aId < bId ? -1 : (aId > bId ? 1 : 0);
    }
    if (aIsNum) return -1;
    if (bIsNum) return 1;

    return aId < bId ? -1 : (aId > bId ? 1 : 0);
  });

  dataRange.setValues(data);
}


function compareLooseNatural_(a, b) {
  var left = String(a === null || a === undefined ? '' : a).trim();
  var right = String(b === null || b === undefined ? '' : b).trim();
  if (left === right) return 0;

  var leftTokens = left.match(/\d+|\D+/g) || [''];
  var rightTokens = right.match(/\d+|\D+/g) || [''];
  var maxLen = Math.max(leftTokens.length, rightTokens.length);

  for (var i = 0; i < maxLen; i++) {
    var lt = leftTokens[i];
    var rt = rightTokens[i];
    if (lt === undefined) return -1;
    if (rt === undefined) return 1;
    if (lt === rt) continue;

    var ltIsNum = /^\d+$/.test(lt);
    var rtIsNum = /^\d+$/.test(rt);

    if (ltIsNum && rtIsNum) {
      var lNum = Number(lt);
      var rNum = Number(rt);
      if (lNum !== rNum) return lNum - rNum;
      if (lt.length !== rt.length) return lt.length - rt.length;
      continue;
    }

    var lText = lt.toLowerCase();
    var rText = rt.toLowerCase();
    if (lText < rText) return -1;
    if (lText > rText) return 1;
  }

  return left < right ? -1 : 1;
}

function getLoanCreatedAtSortNumber_(value) {
  var dateNumber = getThaiDateNumber_(value);
  return dateNumber > 0 ? dateNumber : 99999999;
}

function sortLoansSheetByRules_(loanSheet) {
  if (!loanSheet || loanSheet.getLastRow() <= 2) return;

  var lastRow = loanSheet.getLastRow();
  var lastColumn = Math.max(loanSheet.getLastColumn(), 12);
  var dataRange = loanSheet.getRange(2, 1, lastRow - 1, lastColumn);
  var data = dataRange.getValues();

  data.sort(function(a, b) {
    var createdCompare = getLoanCreatedAtSortNumber_(a[9]) - getLoanCreatedAtSortNumber_(b[9]);
    if (createdCompare !== 0) return createdCompare;

    var memberCompare = compareLooseNatural_(a[0], b[0]);
    if (memberCompare !== 0) return memberCompare;

    return compareLooseNatural_(a[1], b[1]);
  });

  dataRange.setValues(data);
}

function sortLoansSheetNow() {
  requireAdminSession_();
  var lock = LockService.getScriptLock();
  var lockAcquired = false;
  try {
    lock.waitLock(30000);
    lockAcquired = true;
    var ss = getDatabaseSpreadsheet_();
    var loanSheet = getSheetOrThrow(ss, "Loans");
    sortLoansSheetByRules_(loanSheet);
    clearAppCache_();
    return {
      status: "Success",
      message: "จัดเรียงชีต Loans เรียบร้อยแล้ว"
    };
  } catch (e) {
    return {
      status: "Error",
      message: "ไม่สามารถจัดเรียงชีต Loans ได้: " + e.toString()
    };
  } finally {
    if (lockAcquired) lock.releaseLock();
  }
}

// ใช้ hash สำหรับรหัสผ่าน
function hashText(text) {
  var raw = Utilities.computeDigest(
    Utilities.DigestAlgorithm.SHA_256,
    text,
    Utilities.Charset.UTF_8
  );
  return raw.map(function(b) {
    var v = (b < 0 ? b + 256 : b).toString(16);
    return v.length === 1 ? "0" + v : v;
  }).join("");
}

function generatePasswordSalt_() {
  return Utilities.getUuid().replace(/-/g, '');
}

function buildStrongPasswordHash_(password, salt, iterations) {
  var safePassword = String(password || '');
  var safeSalt = String(salt || '');
  var rounds = Math.max(1000, Number(iterations) || 10000);
  var digestInput = safePassword + '|' + safeSalt;

  for (var i = 0; i < rounds; i++) {
    digestInput = hashText(digestInput);
  }

  return digestInput;
}

function createPasswordHashRecord_(password) {
  var iterations = 10000;
  var salt = generatePasswordSalt_();
  return 'v2$' + iterations + '$' + salt + '$' + buildStrongPasswordHash_(password, salt, iterations);
}

function verifyPasswordAgainstStoredHash_(password, storedHash) {
  var safeStoredHash = String(storedHash || '').trim();
  if (!safeStoredHash) return false;

  if (/^v2\$\d+\$[A-Za-z0-9]+\$[a-f0-9]{64}$/i.test(safeStoredHash)) {
    var parts = safeStoredHash.split('$');
    var iterations = Number(parts[1]) || 10000;
    var salt = parts[2] || '';
    var expected = parts[3] || '';
    return buildStrongPasswordHash_(password, salt, iterations) === expected;
  }

  return hashText(password || '') === safeStoredHash;
}

function isLegacyPasswordHash_(storedHash) {
  return /^[a-f0-9]{64}$/i.test(String(storedHash || '').trim());
}

// ==========================================
// 1. ดึงข้อมูลจากฐานข้อมูล (READ)
// ==========================================


function buildDashboardOverviewStatsFromLoans_(loansData, totalMembers) {
  var loans = Array.isArray(loansData) ? loansData : [];
  var summary = {
    totalLoanAmount: 0,
    totalOutstandingBalance: 0,
    totalMembers: Math.max(0, Number(totalMembers) || 0),
    activeLoanCount: 0,
    pendingApprovalCount: 0,
    nplCount: 0,
    nplRatio: '0.0'
  };
  if (!loans.length) return summary;

  for (var i = 0; i < loans.length; i++) {
    var loan = loans[i] || {};
    var amount = Number(loan.amount) || 0;
    var balance = Number(loan.balance) || 0;
    var status = String(loan.status || '').trim();
    summary.totalLoanAmount += amount;
    if (balance > 0 && status !== 'ปิดบัญชี' && status.indexOf('กลบหนี้') === -1) {
      summary.activeLoanCount += 1;
      summary.totalOutstandingBalance += balance;
    }
    if (status === 'รออนุมัติ') summary.pendingApprovalCount += 1;
    if (status.indexOf('ค้างชำระ') !== -1) summary.nplCount += 1;
  }

  summary.nplRatio = loans.length > 0 ? ((summary.nplCount / loans.length) * 100).toFixed(1) : '0.0';
  return summary;
}

function buildAppRuntimeSnapshot_(ss, now) {
  var spreadsheet = ss || getDatabaseSpreadsheet_();
  var asOfDate = now || new Date();

  var settingsSheet = spreadsheet.getSheetByName("Settings");
  var interestRate = settingsSheet ? (Number(getSettingValue_(settingsSheet, 'InterestRate')) || 12.0) : 12.0;
  var lastClosedAccountingMonth = settingsSheet ? String(getSettingValue_(settingsSheet, 'LastClosedAccountingMonth') || '').trim() : '';
  var lastClosedAccountingAt = settingsSheet ? String(getSettingValue_(settingsSheet, 'LastClosedAccountingAt') || '').trim() : '';
  var operatingDayCalendar = settingsSheet ? getOperatingDayCalendar_(settingsSheet) : {};
  var reportLayoutSettings = settingsSheet ? getReportLayoutSettings_(settingsSheet) : cloneReportLayoutDefaults_();

  var loanSheet = spreadsheet.getSheetByName("Loans");
  var loanRows = [];
  if (loanSheet) {
    var loanLastRow = loanSheet.getLastRow();
    if (loanLastRow > 1) {
      loanRows = loanSheet.getRange(2, 1, loanLastRow - 1, 12).getValues();
    }
  }

  var currentMonthContext = (function() {
    var nowParts = parseThaiDateParts_(asOfDate) || { month: asOfDate.getMonth() + 1, yearBe: asOfDate.getFullYear() + 543 };
    return {
      month: nowParts.month,
      yearBe: nowParts.yearBe,
      monthKey: nowParts.yearBe + '-' + padNumber_(nowParts.month, 2)
    };
  })();

  var paidInstallmentsByContract = collectInterestInstallmentsForYearAcrossSources_(spreadsheet, currentMonthContext.yearBe);
  var dashboardMonthlyCover = getMonthlyCoverSummariesForDashboard_(spreadsheet, asOfDate, loanRows);
  var includeCurrentMonthInstallmentInRuntime = shouldCountSelectedMonthAsDueForOverdueReport_(currentMonthContext.yearBe, currentMonthContext.month, lastClosedAccountingMonth);

  var loansData = [];
  if (loanRows.length > 0) {
    var annualStateIndex = buildLoanAnnualInterestStateIndexFromRows_(loanRows, paidInstallmentsByContract, asOfDate);
    loansData = loanRows.map(function(row) {
      var contractNo = String(row[1] || "").trim();
      var annualState = annualStateIndex[contractNo] || calculateLoanAnnualInterestState_(row[9], 0, asOfDate, row[5], row[6]);
      var baseMissedInterestMonths = Math.max(0, Number(annualState.missedInterestMonths) || 0);
      var currentMonthRequiredInstallment = includeCurrentMonthInstallmentInRuntime && !annualState.currentMonthCovered ? 1 : 0;
      var effectiveMissedInterestMonths = baseMissedInterestMonths + currentMonthRequiredInstallment;
      return {
        memberId: String(row[0] || ""),
        contract: contractNo,
        member: String(row[2] || ""),
        amount: Number(row[3]) || 0,
        interest: Number(row[4]) || 0,
        balance: Number(row[5]) || 0,
        status: deriveLoanStatusFromState_(Number(row[5]) || 0, effectiveMissedInterestMonths, String(row[6] || '')),
        nextPayment: String(row[7] || ""),
        missedInterestMonths: baseMissedInterestMonths,
        reportMissedInterestMonths: baseMissedInterestMonths + currentMonthRequiredInstallment,
        currentMonthRequiredInstallment: currentMonthRequiredInstallment,
        currentMonthInterestPaid: !!annualState.currentMonthCovered,
        currentMonthKey: currentMonthContext.monthKey,
        paidInterestInstallmentsYtd: Math.max(0, Number(annualState.paidInstallmentsYtd) || 0),
        dueInterestInstallmentsYtd: Math.max(0, Number(annualState.dueMonthsYtd) || 0),
        maxInterestInstallments: Math.max(1, Number(annualState.maxAllowedPaymentInstallments) || 1),
        createdAt: normalizeLoanCreatedAt_(row[9]),
        guarantor1: String(row[10] || ""),
        guarantor2: String(row[11] || "")
      };
    });
  }

  var transactions = [];
  var currentTransactionSheets = listCurrentTransactionSheets_(spreadsheet);
  if (currentTransactionSheets.length > 0) {
    var recentRows = [];
    for (var ts = 0; ts < currentTransactionSheets.length; ts++) {
      var transSheet = currentTransactionSheets[ts];
      ensureTransactionsSheetSchema_(transSheet);
      var transLastRow = transSheet.getLastRow();
      if (transLastRow <= 1) continue;

      var totalRows = transLastRow - 1;
      var readRows = Math.min(totalRows, 100);
      var startRow = transLastRow - readRows + 1;
      var transLastCol = transSheet.getLastColumn();
      var tData = transSheet.getRange(startRow, 1, readRows, transLastCol).getValues();
      for (var tr = 0; tr < tData.length; tr++) {
        recentRows.push(tData[tr]);
      }
    }

    var seenRecentKeys = {};
    transactions = recentRows
      .filter(function(row) { return row[10] !== "ยกเลิก"; })
      .map(function(row) {
        var timestamp = normalizeThaiDateCellValue_(row[1], true);
        var dateOnly = String(timestamp || '').split(' ')[0] || '';
        var dateNumber = getThaiDateNumber_(dateOnly);
        var timeMatch = String(timestamp || '').match(/(\d{1,2}):(\d{2})(?::(\d{2}))?$/);
        var hh = timeMatch ? padNumber_(Number(timeMatch[1]) || 0, 2) : '00';
        var mm = timeMatch ? padNumber_(Number(timeMatch[2]) || 0, 2) : '00';
        var ssPart = timeMatch ? padNumber_(Number(timeMatch[3]) || 0, 2) : '00';
        return {
          id: String(row[0] || ""),
          timestamp: timestamp,
          contract: String(row[2] || ""),
          memberId: String(row[3] || ""),
          principalPaid: Number(row[4]) || 0,
          interestPaid: Number(row[5]) || 0,
          newBalance: Number(row[6]) || 0,
          note: String(row[7] || ""),
          memberName: String(row[9] || ""),
          txStatus: String(row[10] || "ปกติ").trim() || "ปกติ",
          interestMonthsPaid: Number(row[11]) || 0,
          missedBeforePayment: Number(row[12]) || 0,
          missedAfterPayment: Number(row[13]) || 0,
          _sortKey: String(dateNumber || 0) + hh + mm + ssPart
        };
      })
      .sort(function(a, b) {
        return String(b._sortKey || '').localeCompare(String(a._sortKey || ''));
      })
      .filter(function(tx) {
        var dedupeKey = String(tx.id || '').trim() || [
          String(tx.timestamp || '').trim(),
          String(tx.contract || '').trim(),
          String(tx.memberId || '').trim(),
          Number(tx.principalPaid) || 0,
          Number(tx.interestPaid) || 0,
          Number(tx.newBalance) || 0,
          String(tx.note || '').trim()
        ].join('|');
        if (seenRecentKeys[dedupeKey]) return false;
        seenRecentKeys[dedupeKey] = true;
        return true;
      })
      .slice(0, 100);
  }

  var dashboardOverviewStats = buildDashboardOverviewStatsFromLoans_(loansData, 0);

  return {
    status: 'Success',
    loans: loansData,
    transactions: transactions,
    monthlyDashboardCoverCards: dashboardMonthlyCover.cards || [],
    dashboardSummaryYearBe: dashboardMonthlyCover.yearBe || currentMonthContext.yearBe,
    dashboardOverviewStats: dashboardOverviewStats,
    settings: {
      interestRate: interestRate,
      dailyInterestAutomationEnabled: false,
      dailyInterestAutomationSchedule: 'คำนวณค้างดอกเมื่อกดปิดยอดสิ้นวัน',
      lastClosedAccountingMonth: lastClosedAccountingMonth,
      lastClosedAccountingAt: lastClosedAccountingAt,
      operatingDayCalendar: operatingDayCalendar,
      reportLayoutSettings: getReportLayoutSettings_(settingsSheet)
    }
  };
}

function getAppRuntimeSnapshot() {
  requireAuthenticatedSession_();
  var cacheKey = 'appRuntimeSnapshot';
  var cachedData = getJsonCache_(cacheKey);
  if (cachedData) return cachedData;

  var payload = buildAppRuntimeSnapshot_(getDatabaseSpreadsheet_(), new Date());
  putJsonCache_(cacheKey, payload, 120);
  return payload;
}


function getAppData() {
  var sessionData = requireAuthenticatedSession_();
  var cachedData = getJsonCache_('appDataCache');
  if (cachedData) {
    var cachedSnapshot = getUserAuthorizationSnapshot_(sessionData.username);
    var cachedSettings = cachedData.settings || {};
    cachedData.currentUser = {
      userId: String((cachedSnapshot && cachedSnapshot.userId) || sessionData.userId || '').trim(),
      username: String((cachedSnapshot && cachedSnapshot.username) || sessionData.username || '').trim(),
      fullName: String((cachedSnapshot && cachedSnapshot.fullName) || sessionData.fullName || '').trim(),
      role: String((cachedSnapshot && cachedSnapshot.role) || sessionData.role || 'staff').trim(),
      permissions: (cachedSnapshot && cachedSnapshot.permissions) || [],
      permissionCatalog: getPermissionCatalog_()
    };
    cachedData.settings = {
      interestRate: Number(cachedSettings.interestRate) || 12.0,
      dailyInterestAutomationEnabled: false,
      dailyInterestAutomationSchedule: 'คำนวณค้างดอกเมื่อกดปิดยอดสิ้นวัน',
      lastClosedAccountingMonth: String(cachedSettings.lastClosedAccountingMonth || '').trim(),
      lastClosedAccountingAt: String(cachedSettings.lastClosedAccountingAt || '').trim(),
      operatingDayCalendar: normalizeOperatingDayCalendar_(cachedSettings.operatingDayCalendar),
      reportLayoutSettings: normalizeReportLayoutSettings_(cachedSettings.reportLayoutSettings)
    };
    return cachedData;
  }

  var ss = getDatabaseSpreadsheet_();
  var now = new Date();

  var settingsSheet = ss.getSheetByName("Settings");
  var interestRate = settingsSheet ? (Number(getSettingValue_(settingsSheet, 'InterestRate')) || 12.0) : 12.0;
  var lastClosedAccountingMonth = settingsSheet ? String(getSettingValue_(settingsSheet, 'LastClosedAccountingMonth') || '').trim() : '';
  var lastClosedAccountingAt = settingsSheet ? String(getSettingValue_(settingsSheet, 'LastClosedAccountingAt') || '').trim() : '';
  var operatingDayCalendar = settingsSheet ? getOperatingDayCalendar_(settingsSheet) : {};
  var reportLayoutSettings = settingsSheet ? getReportLayoutSettings_(settingsSheet) : cloneReportLayoutDefaults_();

  var loanSheet = ss.getSheetByName("Loans");
  var loanRows = [];
  if (loanSheet) {
    var loanLastRow = loanSheet.getLastRow();
    if (loanLastRow > 1) {
      var loanRowCount = loanLastRow - 1;
      loanRows = loanSheet.getRange(2, 1, loanRowCount, 12).getValues();
    }
  }

  var currentMonthContext = (function() {
    var nowParts = parseThaiDateParts_(now) || { month: now.getMonth() + 1, yearBe: now.getFullYear() + 543 };
    return {
      month: nowParts.month,
      yearBe: nowParts.yearBe,
      monthKey: nowParts.yearBe + '-' + padNumber_(nowParts.month, 2)
    };
  })();
  var paidInstallmentsByContract = collectInterestInstallmentsForYearAcrossSources_(ss, currentMonthContext.yearBe);
  var dashboardMonthlyCover = getMonthlyCoverSummariesForDashboard_(ss, now, loanRows);
  var includeCurrentMonthInstallmentInRuntime = shouldCountSelectedMonthAsDueForOverdueReport_(currentMonthContext.yearBe, currentMonthContext.month, lastClosedAccountingMonth);

  var loansData = [];
  if (loanRows.length > 0) {
      var annualStateIndex = buildLoanAnnualInterestStateIndexFromRows_(loanRows, paidInstallmentsByContract, now);
      loansData = loanRows.map(function(row) {
        var contractNo = String(row[1] || "").trim();
        var annualState = annualStateIndex[contractNo] || calculateLoanAnnualInterestState_(row[9], 0, now, row[5], row[6]);
        var baseMissedInterestMonths = Math.max(0, Number(annualState.missedInterestMonths) || 0);
        var currentMonthRequiredInstallment = includeCurrentMonthInstallmentInRuntime && !annualState.currentMonthCovered ? 1 : 0;
        var effectiveMissedInterestMonths = baseMissedInterestMonths + currentMonthRequiredInstallment;
        return {
          memberId: String(row[0] || ""),
          contract: contractNo,
          member: String(row[2] || ""),
          amount: Number(row[3]) || 0,
          interest: Number(row[4]) || 0,
          balance: Number(row[5]) || 0,
          status: deriveLoanStatusFromState_(Number(row[5]) || 0, effectiveMissedInterestMonths, String(row[6] || "")),
          nextPayment: String(row[7] || ""),
          missedInterestMonths: baseMissedInterestMonths,
          reportMissedInterestMonths: baseMissedInterestMonths + currentMonthRequiredInstallment,
          currentMonthRequiredInstallment: currentMonthRequiredInstallment,
          currentMonthInterestPaid: !!annualState.currentMonthCovered,
          currentMonthKey: currentMonthContext.monthKey,
          paidInterestInstallmentsYtd: Math.max(0, Number(annualState.paidInstallmentsYtd) || 0),
          dueInterestInstallmentsYtd: Math.max(0, Number(annualState.dueMonthsYtd) || 0),
          maxInterestInstallments: Math.max(1, Number(annualState.maxAllowedPaymentInstallments) || 1),
          createdAt: normalizeLoanCreatedAt_(row[9]),
          guarantor1: String(row[10] || ""),
          guarantor2: String(row[11] || "")
        };
      });
  }

  var memberSheet = ss.getSheetByName("Members");
  var membersData = [];
  if (memberSheet) {
    var memberLastRow = memberSheet.getLastRow();
    if (memberLastRow > 1) {
      var memberRowCount = memberLastRow - 1;
      var mData = memberSheet.getRange(2, 1, memberRowCount, 3).getValues();
      membersData = mData.map(function(row) {
        return {
          id: String(row[0] || ""),
          name: String(row[1] || ""),
          status: String(row[2] || "")
        };
      });
    }
  }

  var transDataArr = [];
  var currentTransactionSheets = listCurrentTransactionSheets_(ss);
  if (currentTransactionSheets.length > 0) {
    var recentRows = [];
    for (var ts = 0; ts < currentTransactionSheets.length; ts++) {
      var transSheet = currentTransactionSheets[ts];
      ensureTransactionsSheetSchema_(transSheet);
      var transLastRow = transSheet.getLastRow();
      if (transLastRow <= 1) continue;

      var totalRows = transLastRow - 1;
      var readRows = Math.min(totalRows, 100);
      var startRow = transLastRow - readRows + 1;
      var transLastCol = transSheet.getLastColumn();
      var tData = transSheet.getRange(startRow, 1, readRows, transLastCol).getValues();
      for (var tr = 0; tr < tData.length; tr++) {
        recentRows.push(tData[tr]);
      }
    }

    var seenRecentKeys = {};
    transDataArr = recentRows
      .filter(function(row) { return row[10] !== "ยกเลิก"; })
      .map(function(row) {
        var timestamp = normalizeThaiDateCellValue_(row[1], true);
        var dateOnly = String(timestamp || '').split(' ')[0] || '';
        var dateNumber = getThaiDateNumber_(dateOnly);
        var timeMatch = String(timestamp || '').match(/(\d{1,2}):(\d{2})(?::(\d{2}))?$/);
        var hh = timeMatch ? padNumber_(Number(timeMatch[1]) || 0, 2) : '00';
        var mm = timeMatch ? padNumber_(Number(timeMatch[2]) || 0, 2) : '00';
        var ss = timeMatch ? padNumber_(Number(timeMatch[3]) || 0, 2) : '00';
        return {
          id: String(row[0] || ""),
          timestamp: timestamp,
          contract: String(row[2] || ""),
          memberId: String(row[3] || ""),
          principalPaid: Number(row[4]) || 0,
          interestPaid: Number(row[5]) || 0,
          newBalance: Number(row[6]) || 0,
          note: String(row[7] || ""),
          memberName: String(row[9] || ""),
          txStatus: String(row[10] || "ปกติ").trim() || "ปกติ",
          interestMonthsPaid: Number(row[11]) || 0,
          missedBeforePayment: Number(row[12]) || 0,
          missedAfterPayment: Number(row[13]) || 0,
          _sortKey: String(dateNumber || 0) + hh + mm + ss
        };
      })
      .sort(function(a, b) {
        return String(b._sortKey || '').localeCompare(String(a._sortKey || ''));
      })
      .filter(function(tx) {
        var dedupeKey = String(tx.id || '').trim() || [
          String(tx.timestamp || '').trim(),
          String(tx.contract || '').trim(),
          String(tx.memberId || '').trim(),
          Number(tx.principalPaid) || 0,
          Number(tx.interestPaid) || 0,
          Number(tx.newBalance) || 0,
          String(tx.note || '').trim()
        ].join('|');
        if (seenRecentKeys[dedupeKey]) return false;
        seenRecentKeys[dedupeKey] = true;
        return true;
      })
      .slice(0, 100);
  }

  var currentUserSnapshot = getUserAuthorizationSnapshot_(sessionData.username);
  var dashboardOverviewStats = buildDashboardOverviewStatsFromLoans_(loansData, membersData.length);

  var appData = {
    loans: loansData,
    members: membersData,
    transactions: transDataArr,
    monthlyDashboardCoverCards: dashboardMonthlyCover.cards || [],
    dashboardSummaryYearBe: dashboardMonthlyCover.yearBe || currentMonthContext.yearBe,
    dashboardOverviewStats: dashboardOverviewStats,
    currentUser: {
      userId: String((currentUserSnapshot && currentUserSnapshot.userId) || sessionData.userId || '').trim(),
      username: String((currentUserSnapshot && currentUserSnapshot.username) || sessionData.username || '').trim(),
      fullName: String((currentUserSnapshot && currentUserSnapshot.fullName) || sessionData.fullName || '').trim(),
      role: String((currentUserSnapshot && currentUserSnapshot.role) || sessionData.role || 'staff').trim(),
      permissions: (currentUserSnapshot && currentUserSnapshot.permissions) || [],
      permissionCatalog: getPermissionCatalog_()
    },
    settings: {
      interestRate: interestRate,
      dailyInterestAutomationEnabled: false,
      dailyInterestAutomationSchedule: 'คำนวณค้างดอกเมื่อกดปิดยอดสิ้นวัน',
      lastClosedAccountingMonth: lastClosedAccountingMonth,
      lastClosedAccountingAt: lastClosedAccountingAt,
      operatingDayCalendar: operatingDayCalendar,
      reportLayoutSettings: reportLayoutSettings
    }
  };

  putJsonCache_('appDataCache', appData, 300);
  return appData;
}


function getPaymentLookupByMemberId(memberId) {
  requirePermission_('payments.create', 'ไม่มีสิทธิ์ค้นหาข้อมูลรับชำระ');
  try {
    var normalizedMemberId = String(memberId || '').trim();
    if (!normalizedMemberId) {
      return { status: 'Error', message: 'กรุณาระบุรหัสสมาชิก', loans: [], todayTransactions: [] };
    }

    var cacheKey = getPaymentMemberLookupCacheKey_(normalizedMemberId);
    var cached = getJsonCache_(cacheKey);
    if (cached && cached.memberId === normalizedMemberId) {
      return cached;
    }

    var ss = getDatabaseSpreadsheet_();
    var interestRate = getPaymentInterestRateCached_(ss);
    var loans = getIndexedPaymentLoansByMemberIdFast_(ss, normalizedMemberId);
    var payload = {
      status: 'Success',
      memberId: normalizedMemberId,
      memberName: loans.length ? String(loans[0].member || '').trim() : '',
      loans: loans,
      todayTransactions: [],
      interestRate: interestRate
    };
    putJsonCache_(cacheKey, payload, 30);
    return payload;
  } catch (e) {
    return {
      status: 'Error',
      message: 'ค้นหาข้อมูลรับชำระไม่สำเร็จ: ' + e.toString(),
      loans: [],
      todayTransactions: []
    };
  }
}

function getPaymentReadyData() {
  var sessionData = requireAuthenticatedSession_();
  var cacheKey = 'paymentReadyDataCache';
  var cachedData = getJsonCache_(cacheKey);
  if (cachedData) return cachedData;

  var ss = getDatabaseSpreadsheet_();
  var interestRate = getPaymentInterestRateCached_(ss);
  var indexSheet = ensureFreshPaymentSearchIndexSheet_(ss);
  var loansData = [];

  if (indexSheet && indexSheet.getLastRow() > 1) {
    var values = indexSheet.getRange(2, 1, indexSheet.getLastRow() - 1, PAYMENT_SEARCH_INDEX_HEADERS.length).getValues();
    for (var i = 0; i < values.length; i++) {
      loansData.push(buildPaymentLoanFromIndexValues_(values[i]));
    }
  }

  var payload = {
    status: 'Success',
    loans: loansData,
    settings: {
      interestRate: interestRate
    }
  };

  putJsonCache_(cacheKey, payload, 120);
  return payload;
}

function getTodayTransactionsForMemberPaymentEdit(memberId) {
  requirePermission_('payments.create', 'ไม่มีสิทธิ์ตรวจสอบรายการรับชำระ');
  try {
    var normalizedMemberId = String(memberId || '').trim();
    if (!normalizedMemberId) {
      return { status: 'Error', message: 'กรุณาระบุรหัสสมาชิก', transactions: [] };
    }

    var cacheKey = getPaymentMemberTodayCacheKey_(normalizedMemberId);
    var cached = getJsonCache_(cacheKey);
    if (cached && cached.memberId === normalizedMemberId) {
      return cached;
    }

    var ss = getDatabaseSpreadsheet_();
    var rows = getIndexedTodayTransactionsByMemberIdFast_(ss, normalizedMemberId);
    var payload = {
      status: 'Success',
      memberId: normalizedMemberId,
      memberName: rows.length > 0 ? String(rows[0].memberName || '').trim() : '',
      transactions: rows
    };
    putJsonCache_(cacheKey, payload, 15);
    return payload;
  } catch (e) {
    return {
      status: 'Error',
      message: 'โหลดรายการรับชำระของสมาชิกไม่สำเร็จ: ' + e.toString(),
      transactions: []
    };
  }
}

function buildTransactionOrderKey_(rowValues, rowIndex) {
  var timestamp = normalizeThaiDateCellValue_(rowValues && rowValues[1], true);
  var dateOnly = String(timestamp || '').split(' ')[0] || '';
  var dateNumber = getThaiDateNumber_(dateOnly);
  var timeMatch = String(timestamp || '').match(/(\d{1,2}):(\d{2})(?::(\d{2}))?$/);
  var hh = timeMatch ? padNumber_(Number(timeMatch[1]) || 0, 2) : '00';
  var mm = timeMatch ? padNumber_(Number(timeMatch[2]) || 0, 2) : '00';
  var ss = timeMatch ? padNumber_(Number(timeMatch[3]) || 0, 2) : '00';
  var rowKey = padNumber_(Number(rowIndex) || 0, 6);
  return String(dateNumber || 0) + hh + mm + ss + rowKey;
}

function hasLaterActiveTransactionForContract_(ss, matchedTx, contractNo) {
  if (!ss || !matchedTx || !contractNo) return false;

  var currentTxId = String((matchedTx.values || [])[0] || '').trim();
  var currentOrderKey = buildTransactionOrderKey_(matchedTx.values || [], matchedTx.rowIndex);
  var currentSheetName = matchedTx.sheet ? String(matchedTx.sheet.getName() || '') : '';
  var targetContract = String(contractNo || '').trim();
  if (!targetContract) return false;

  var currentSheetNames = getCurrentTransactionSheetNameSet_(ss);
  var indexedRows = getIndexedContractTransactionRows_(ss, targetContract);
  for (var i = 0; i < indexedRows.length; i++) {
    var rowMeta = indexedRows[i];
    var rowSheetName = String(rowMeta.sheet && rowMeta.sheet.getName ? rowMeta.sheet.getName() || '' : '').trim();
    if (!currentSheetNames[rowSheetName]) continue;

    var row = rowMeta.values || [];
    if (isInactiveTransactionStatus_(String(row[10] || '').trim())) continue;

    var rowTxId = String(row[0] || '').trim();
    if (rowTxId === currentTxId && Number(rowMeta.rowIndex) === Number(matchedTx.rowIndex) && rowSheetName === currentSheetName) {
      continue;
    }

    var candidateOrderKey = buildTransactionOrderKey_(row, rowMeta.rowIndex);
    if (candidateOrderKey > currentOrderKey) {
      return true;
    }
  }

  return false;
}

function syncLoanStateFromTransactionRow_(loanSheet, transactionRowValues) {
  if (!loanSheet || !transactionRowValues || transactionRowValues.length < 14) return null;

  var contractNo = String(transactionRowValues[2] || '').trim();
  if (!contractNo) return null;

  var loanRowIndex = findRowByExactValue(loanSheet, 2, contractNo);
  if (!loanRowIndex) return null;

  var existingNextPayment = loanSheet.getRange(loanRowIndex, 8).getValue();
  var newBalance = Number(transactionRowValues[6]) || 0;
  var missedAfter = Math.max(0, Number(transactionRowValues[13]) || 0);
  var noteText = String(transactionRowValues[7] || '').trim();
  var derivedStatus = deriveLoanStatusFromState_(newBalance, missedAfter, noteText.indexOf('กลบหนี้') !== -1 ? 'กลบหนี้' : 'ปกติ');

  loanSheet.getRange(loanRowIndex, 6, 1, 4).setValues([[
    newBalance,
    derivedStatus,
    existingNextPayment,
    missedAfter
  ]]);

  return {
    rowIndex: loanRowIndex,
    balance: newBalance,
    status: derivedStatus,
    missedInterestMonths: missedAfter
  };
}

function savePayment(paymentDataStr) {
  requirePermission_('payments.create', 'ไม่มีสิทธิ์บันทึกรับชำระเงิน');

  var lock = LockService.getScriptLock();
  var lockAcquired = false;
  try {
    lock.waitLock(30000);
    lockAcquired = true;

    var ss = getDatabaseSpreadsheet_();
    var paymentData = JSON.parse(paymentDataStr);

    var transSheet = resolveWritableTransactionsSheet_(ss);
    var loanSheet = getSheetOrThrow(ss, "Loans");
    ensureTransactionsSheetSchema_(transSheet);

    var txId = String(paymentData.id || Utilities.getUuid()).trim();
    if (!txId) txId = Utilities.getUuid();

    var existingTx = findExistingTransactionById_(ss, txId);
    if (existingTx) {
      if (!isDuplicatePaymentPayloadConsistent_(existingTx.values, paymentData)) {
        throw new Error('พบรหัสรายการซ้ำ แต่ข้อมูลรายการไม่ตรงกับที่มีอยู่เดิม ระบบยกเลิกการบันทึกเพื่อป้องกันข้อมูลเสียหาย');
      }
      try {
        syncLoanStateFromTransactionRow_(loanSheet, existingTx.values);
      } catch (syncError) {
        console.warn('syncLoanStateFromTransactionRow_ failed for duplicate tx', syncError);
      }
      syncPaymentTodayIndexEntries_(ss, [{
        txId: txId,
        rowValues: existingTx.values,
        rowIndex: existingTx.rowIndex
      }]);
      clearAppCache_();
      return {
        status: "Success",
        transactionId: txId,
        timestamp: existingTx.values[1] || getThaiDate(new Date()).dateTime,
        duplicated: true
      };
    }

    var requestedContractNo = String(paymentData.contract || "").trim();
    if (!requestedContractNo) throw new Error("กรุณาระบุเลขที่สัญญา");

    var loanRowIndex = findRowByExactValue(loanSheet, 2, requestedContractNo);
    if (!loanRowIndex) throw new Error("ไม่พบเลขที่สัญญา: " + requestedContractNo);

    var loanRow = loanSheet.getRange(loanRowIndex, 1, 1, 12).getValues()[0];
    var authoritativeMemberId = String(loanRow[0] || "").trim();
    var authoritativeMemberName = String(loanRow[2] || "").trim();
    var principalPaid = Number(paymentData.principalPaid);
    var interestPaid = Number(paymentData.interestPaid);
    var newBalance = Number(paymentData.newBalance);

    if (!isFinite(principalPaid) || !isFinite(interestPaid) || !isFinite(newBalance)) {
      throw new Error("ยอดรับชำระต้องเป็นตัวเลขที่ถูกต้อง");
    }
    if (principalPaid < 0 || interestPaid < 0) throw new Error("ยอดรับชำระติดลบไม่ได้");
    if ((principalPaid + interestPaid) <= 0) throw new Error("กรุณาระบุยอดรับชำระอย่างน้อย 1 รายการ");

    var currentBalanceInSheet = Number(loanRow[5]) || 0;
    var annualInterestRate = Number(loanRow[4]);
    if (!isFinite(annualInterestRate) || annualInterestRate < 0) annualInterestRate = 12.0;
    if (principalPaid > currentBalanceInSheet) throw new Error("ยอดชำระเงินต้นเกินยอดคงค้าง");

    var rawNote = normalizeTextForMatch_(paymentData.note || "");
    var rawRequestedStatus = normalizeTextForMatch_(paymentData.customStatus || "");
    var isOffsetPayment = rawRequestedStatus.indexOf("กลบหนี้") !== -1 || rawNote.indexOf("กลบหนี้") !== -1;
    var now = new Date();
    var todayThaiDate = getThaiDate(now).dateOnly;

    if (!isOffsetPayment) {
      if (hasActivePaymentForContractOnThaiDate_(ss, requestedContractNo, todayThaiDate)) {
        throw new Error("เลขที่สัญญานี้มีรายการรับชำระแล้วในวันนี้");
      }
    }

    if (isOffsetPayment) {
      if (principalPaid !== currentBalanceInSheet) throw new Error("การกลบหนี้ต้องชำระเต็มยอดคงค้างของสัญญา");
      if (interestPaid !== 0) throw new Error("การกลบหนี้ไม่อนุญาตให้บันทึกยอดดอกเบี้ยในรายการเดียวกัน");
    }

    var currentThaiParts = parseThaiDateParts_(now) || { yearBe: now.getFullYear() + 543 };
    var currentYearBe = Number(currentThaiParts.yearBe) || (now.getFullYear() + 543);
    var paidInstallmentsBefore = getInterestInstallmentsPaidForContractInYear_(ss, requestedContractNo, currentYearBe);
    var annualStateBefore = calculateLoanAnnualInterestState_(loanRow[9], paidInstallmentsBefore, now, currentBalanceInSheet, loanRow[6]);

    var expectedMonthlyInterest = Math.floor(currentBalanceInSheet * (annualInterestRate / 100) / 12);
    var interestMonthsPaid = isOffsetPayment ? 0 : Math.max(1, Number(paymentData.interestMonthsPaid) || 1);

    if (!isOffsetPayment && annualStateBefore.maxAllowedPaymentInstallments <= 0) {
      throw new Error("สัญญานี้ชำระดอกเบี้ยล่วงหน้าครบตามปีปัจจุบันแล้ว");
    }
    if (!isOffsetPayment && interestMonthsPaid > Math.max(1, annualStateBefore.maxAllowedPaymentInstallments)) {
      throw new Error("จำนวนงวดดอกที่รับชำระเกินกว่าที่ระบบอนุญาต (สูงสุด " + Math.max(1, annualStateBefore.maxAllowedPaymentInstallments) + " งวด)");
    }
    if (!isOffsetPayment && expectedMonthlyInterest > 0) {
      var expectedInterestPaid = expectedMonthlyInterest * interestMonthsPaid;
      if (interestPaid !== expectedInterestPaid) {
        throw new Error("ยอดชำระดอกเบี้ยต้องตรงกับที่ระบบคำนวณอัตโนมัติจากจำนวนงวดดอกที่เลือก");
      }
    }
    if (!isOffsetPayment && principalPaid > 0 && expectedMonthlyInterest > 0 && interestPaid <= 0) {
      throw new Error("การชำระเงินต้นต้องชำระดอกเบี้ยตามที่ระบบคำนวณด้วย");
    }

    var recalculatedNewBalance = Math.max(0, currentBalanceInSheet - principalPaid);
    if (Math.abs(newBalance - recalculatedNewBalance) > 0.0001) newBalance = recalculatedNewBalance;

    var timeInfo = getThaiDate(now);
    var oldMissedInterestMonths = annualStateBefore.missedInterestMonths;
    var annualStateAfter = isOffsetPayment
      ? annualStateBefore
      : calculateLoanAnnualInterestState_(loanRow[9], paidInstallmentsBefore + interestMonthsPaid, now, newBalance, loanRow[6]);
    var newMissedInterestMonths = (isOffsetPayment || shouldExcludeLoanFromArrearsCount_(newBalance, loanRow[6])) ? 0 : annualStateAfter.missedInterestMonths;

    var serverStatus = isOffsetPayment
      ? "ปิดบัญชี(กลบหนี้)"
      : deriveLoanStatusFromState_(newBalance, newMissedInterestMonths, newBalance <= 0 ? "ปิดบัญชี" : "ปกติ");
    var serverNote = rawNote;
    if (isOffsetPayment) {
      serverNote = "กลบหนี้ด้วยเงินฝากสัจจะ";
    } else if (newBalance <= 0) {
      serverNote = "ปิดบัญชี";
    } else {
      serverNote = serverNote.substring(0, 120);
    }

    var actorName = getCurrentActorName_();
    var transactionRowValues = [[
      txId,
      timeInfo.dateTime,
      requestedContractNo,
      authoritativeMemberId,
      principalPaid,
      interestPaid,
      newBalance,
      serverNote,
      actorName,
      authoritativeMemberName,
      "ปกติ",
      interestMonthsPaid,
      oldMissedInterestMonths,
      newMissedInterestMonths
    ]];
    var lastRowTrans = appendRowsSafely_(transSheet, transactionRowValues, 'บันทึกรายการรับชำระ');
    upsertTransactionIndexEntry_(ss, transSheet, lastRowTrans, transactionRowValues[0]);

    var currentNextPayment = loanRow[7];
    loanSheet.getRange(loanRowIndex, 6, 1, 4).setValues([[
      newBalance,
      serverStatus,
      currentNextPayment,
      newMissedInterestMonths
    ]]);
    invalidateSheetExecutionCaches_(loanSheet);

    appendAuditLogSafe_(ss, {
      action: 'SAVE_PAYMENT',
      entityType: 'TRANSACTION',
      entityId: txId,
      referenceNo: txId,
      after: {
        contract: requestedContractNo,
        memberId: authoritativeMemberId,
        memberName: authoritativeMemberName,
        principalPaid: principalPaid,
        interestPaid: interestPaid,
        newBalance: newBalance,
        status: serverStatus,
        note: serverNote,
        interestMonthsPaid: interestMonthsPaid
      },
      details: 'บันทึกรายการรับชำระสำเร็จ'
    });

    syncPaymentSearchIndexForContracts_(ss, [requestedContractNo]);
    syncPaymentTodayIndexEntries_(ss, [{
      txId: txId,
      rowValues: transactionRowValues[0],
      rowIndex: lastRowTrans
    }]);
    clearAppCache_();
    syncTransactionIndexVersionToCache_();
    syncPaymentSearchIndexVersionToCache_();
    syncPaymentTodayIndexVersionToCache_();
    return {
      status: "Success",
      transactionId: txId,
      timestamp: timeInfo.dateTime,
      transaction: {
        id: txId,
        contract: requestedContractNo,
        memberId: authoritativeMemberId,
        memberName: authoritativeMemberName,
        principalPaid: principalPaid,
        interestPaid: interestPaid,
        newBalance: newBalance,
        note: serverNote,
        status: serverStatus
      }
    };
  } catch (e) {
    console.error("Error in savePayment:", e);
    return { status: "Error", message: "ระบบขัดข้อง: " + e.toString() };
  } finally {
    if (lockAcquired) lock.releaseLock();
  }
}


function cancelPayment(transactionId, reason) {
  requirePermission_('payments.reverse', 'ไม่มีสิทธิ์ลบรายการรับชำระ');
  var lock = LockService.getScriptLock();
  var lockAcquired = false;

  try {
    lock.waitLock(30000);
    lockAcquired = true;

    var ss = getDatabaseSpreadsheet_();
    var matchedTx = findExistingTransactionById_(ss, transactionId);
    if (!matchedTx) {
      return "Error: ไม่พบข้อมูลรายการ";
    }

    var txRowData = matchedTx.values || [];
    if (isInactiveTransactionStatus_(String(txRowData[10] || '').trim())) {
      return "Error: รายการนี้ถูกยกเลิก/กลับรายการไปแล้ว";
    }

    var contractNo = String(txRowData[2] || '').trim();
    if (!contractNo) return "Error: ไม่พบเลขที่สัญญาของรายการที่ต้องการลบ";

    var removeResult = removeManagedPaymentByMatchedTransaction_(
      ss,
      matchedTx,
      "ลบรายการรับชำระจากหน้าหลัก",
      "ลบรายการรับชำระเรียบร้อยแล้ว และคืนยอดสัญญาอัตโนมัติ"
    );

    return removeResult.status === 'Success'
      ? JSON.stringify(removeResult)
      : ('Error: ' + String(removeResult.message || 'ลบรายการไม่สำเร็จ'));
  } catch (e) {
    console.error("Error in cancelPayment:", e);
    return "Error: " + e.toString();
  } finally {
    if (lockAcquired) lock.releaseLock();
  }
}

// ==========================================
// 3. การจัดการสัญญากู้ (LOANS)
// ==========================================

function addLoan(loanDataStr) {
  requirePermission_('loans.manage', 'ไม่มีสิทธิ์เพิ่มสัญญาเงินกู้');
  var lock = LockService.getScriptLock();
  var lockAcquired = false;

  try {
    lock.waitLock(30000);
    lockAcquired = true;

    var ss = getDatabaseSpreadsheet_();
    var loanData = validateLoanPayloadForWrite_(JSON.parse(loanDataStr));
    var loanSheet = getSheetOrThrow(ss, "Loans");

    if (findRowByExactValue(loanSheet, 2, loanData.contract)) {
      return "Error: เลขที่สัญญานี้มีในระบบแล้ว";
    }

    var createdAt = normalizeLoanCreatedAt_(loanData.createdAt) || getThaiDate().dateOnly;
    var autoApprovedStatus = loanData.status && loanData.status !== "รออนุมัติ" ? loanData.status : "ปกติ";
    var normalizedGuarantors = normalizeLoanGuarantorsForSave_(ss, loanData.guarantor1, loanData.guarantor2);

    appendRowsSafely_(loanSheet, [[
      loanData.memberId,
      loanData.contract,
      loanData.member,
      loanData.amount,
      loanData.interest,
      loanData.balance,
      autoApprovedStatus,
      loanData.nextPayment,
      0,
      createdAt,
      normalizedGuarantors.guarantor1 || "",
      normalizedGuarantors.guarantor2 || ""
    ]], 'เพิ่มสัญญากู้ใหม่');

    if (normalizedGuarantors.shouldSortMembers) {
      sortMembersSheetByMemberId_(getSheetOrThrow(ss, "Members"));
    }
    sortLoansSheetByRules_(loanSheet);

    appendAuditLogSafe_(ss, {
      action: 'ADD_LOAN',
      entityType: 'LOAN',
      entityId: String(loanData.contract || ''),
      referenceNo: String(loanData.contract || ''),
      after: {
        memberId: loanData.memberId,
        memberName: loanData.member,
        amount: loanData.amount,
        balance: loanData.balance,
        status: autoApprovedStatus
      },
      details: 'เพิ่มสัญญากู้ใหม่สำเร็จ'
    });
    syncPaymentSearchIndexForContracts_(ss, [loanData.contract]);
    clearAppCache_();
    return "Success";
  } catch (e) {
    return "Error: " + e.toString();
  } finally {
    if (lockAcquired) lock.releaseLock();
  }
}

function editLoan(loanDataStr) {
  requirePermission_('loans.manage', 'ไม่มีสิทธิ์แก้ไขสัญญาเงินกู้');
  var lock = LockService.getScriptLock();
  var lockAcquired = false;

  try {
    lock.waitLock(30000);
    lockAcquired = true;

    var ss = getDatabaseSpreadsheet_();
    var loanData = validateLoanPayloadForWrite_(JSON.parse(loanDataStr));
    var loanSheet = getSheetOrThrow(ss, "Loans");

    if (loanSheet.getLastRow() <= 1) return "Error: ไม่มีข้อมูลสัญญา";

    var row = findRowByExactValue(loanSheet, 2, loanData.contract);
    if (!row) return "Error: ไม่พบเลขที่สัญญา";

    var previousMemberId = String(loanSheet.getRange(row, 1).getValue() || "").trim();
    var currentCreatedAt = loanSheet.getRange(row, 10).getValue();
    var createdAt = normalizeLoanCreatedAt_(loanData.createdAt || currentCreatedAt) || getThaiDate().dateOnly;
    var normalizedGuarantors = normalizeLoanGuarantorsForSave_(ss, loanData.guarantor1, loanData.guarantor2);

    loanSheet.getRange(row, 1, 1, 12).setValues([[
      loanData.memberId,
      loanData.contract,
      loanData.member,
      loanData.amount,
      loanData.interest,
      loanData.balance,
      loanData.status || "ปกติ",
      loanData.nextPayment,
      loanData.missedInterestMonths,
      createdAt,
      normalizedGuarantors.guarantor1 || "",
      normalizedGuarantors.guarantor2 || ""
    ]]);

    if (normalizedGuarantors.shouldSortMembers) {
      sortMembersSheetByMemberId_(getSheetOrThrow(ss, "Members"));
    }
    sortLoansSheetByRules_(loanSheet);

    syncMemberRegisterFromLoanEdit_(ss, previousMemberId, loanData.memberId, loanData.member);

    appendAuditLogSafe_(ss, {
      action: 'EDIT_LOAN',
      entityType: 'LOAN',
      entityId: String(loanData.contract || ''),
      referenceNo: String(loanData.contract || ''),
      after: {
        memberId: loanData.memberId,
        memberName: loanData.member,
        amount: loanData.amount,
        balance: loanData.balance,
        status: loanData.status || 'ปกติ'
      },
      details: 'แก้ไขข้อมูลสัญญาเงินกู้สำเร็จ'
    });

    syncPaymentSearchIndexForContracts_(ss, [loanData.contract]);
    clearAppCache_();
    return "Success";
  } catch (e) {
    return "Error: " + e.toString();
  } finally {
    if (lockAcquired) lock.releaseLock();
  }
}

function updateLoanStatus(contractNo, status) {
  requirePermission_('loans.manage', 'ไม่มีสิทธิ์อนุมัติหรือปรับสถานะสัญญา');
  var lock = LockService.getScriptLock();
  var lockAcquired = false;
  try {
    lock.waitLock(30000);
    lockAcquired = true;
    var ss = getDatabaseSpreadsheet_();
    var loanSheet = getSheetOrThrow(ss, "Loans");
    var row = findRowByExactValue(loanSheet, 2, contractNo);
    if (row) {
      loanSheet.getRange(row, 7).setValue(status);
      syncPaymentSearchIndexForContracts_(ss, [contractNo]);
      clearAppCache_();
      return "Success";
    }
    return "Error: ไม่พบเลขที่สัญญา";
  } catch(e) {
    return "Error: " + e.toString();
  } finally {
    if (lockAcquired) lock.releaseLock();
  }
}

function deleteLoan(contractNo) {
  requirePermission_('loans.manage', 'ไม่มีสิทธิ์ลบสัญญาเงินกู้');

  var lock = LockService.getScriptLock();
  var lockAcquired = false;
  try {
    lock.waitLock(30000);
    lockAcquired = true;

    var ss = getDatabaseSpreadsheet_();
    var loanSheet = getSheetOrThrow(ss, "Loans");
    var normalizedContractNo = String(contractNo || '').trim();
    var row = findRowByExactValue(loanSheet, 2, normalizedContractNo);

    if (!row) return "Error: ไม่พบเลขที่สัญญา";

    var transactionExists = getIndexedContractTransactionRows_(ss, normalizedContractNo).length > 0;

    if (transactionExists) {
      return "Error: สัญญานี้มีประวัติธุรกรรมแล้ว ไม่อนุญาตให้ลบถาวร";
    }

    loanSheet.deleteRow(row);
    sortLoansSheetByRules_(loanSheet);
    appendAuditLogSafe_(ss, {
      action: 'DELETE_LOAN',
      entityType: 'LOAN',
      entityId: normalizedContractNo,
      referenceNo: normalizedContractNo,
      reason: 'ลบข้อมูลสัญญาที่ไม่มีประวัติธุรกรรม',
      details: 'ลบข้อมูลสัญญาสำเร็จ'
    });
    syncPaymentSearchIndexForContracts_(ss, [normalizedContractNo]);
    clearAppCache_();
    logSystemActionFast(ss, "ลบข้อมูลสัญญา", "เลขที่สัญญา: " + normalizedContractNo);
    return "Success";
  } catch(e) {
    return "Error: " + e.toString();
  } finally {
    if (lockAcquired) lock.releaseLock();
  }
}


// ==========================================
// 4. การจัดการสมาชิก (MEMBERS)
// ==========================================

function addMember(memberDataStr) {
  requirePermission_('members.manage', 'ไม่มีสิทธิ์เพิ่มสมาชิก');
  var lock = LockService.getScriptLock();
  var lockAcquired = false;
  try {
    lock.waitLock(30000);
    lockAcquired = true;
    var ss = getDatabaseSpreadsheet_();
    var memberSheet = getSheetOrThrow(ss, "Members");
    var mData = validateMemberPayloadForWrite_(JSON.parse(memberDataStr));
    var memberId = mData.id;
    var memberName = mData.name;
    var memberStatus = mData.status;

    if (findRowByExactValue(memberSheet, 1, memberId)) {
      return "Duplicate ID";
    }

    var upgraded = upgradeTemporaryMemberToReal_(ss, memberId, memberName, memberStatus);
    if (upgraded) {
      appendAuditLogSafe_(ss, {
        action: 'UPGRADE_TEMP_MEMBER',
        entityType: 'MEMBER',
        entityId: memberId,
        referenceNo: memberId,
        before: { previousTempId: upgraded.previousTempId },
        after: { id: memberId, name: memberName, status: memberStatus },
        details: 'จับคู่ผู้ค้ำชั่วคราวเป็นสมาชิกจริงสำเร็จ'
      });
      clearAppCache_();
      logSystemActionFast(ss, "จับคู่ผู้ค้ำชั่วคราวเป็นสมาชิกจริง", "เปลี่ยน " + upgraded.previousTempId + " เป็น " + memberId + " | " + memberName);
      return {
        status: "Success",
        action: "upgradedTemporaryGuarantor",
        previousTempId: upgraded.previousTempId,
        member: { id: memberId, name: memberName, status: memberStatus }
      };
    }

    appendRowsSafely_(memberSheet, [[
      memberId,
      memberName,
      memberStatus
    ]], 'เพิ่มสมาชิกใหม่');
    sortMembersSheetByMemberId_(memberSheet);
    appendAuditLogSafe_(ss, {
      action: 'ADD_MEMBER',
      entityType: 'MEMBER',
      entityId: memberId,
      referenceNo: memberId,
      after: { id: memberId, name: memberName, status: memberStatus },
      details: 'เพิ่มสมาชิกใหม่สำเร็จ'
    });
    clearAppCache_();
    return {
      status: "Success",
      action: "createdMember",
      member: { id: memberId, name: memberName, status: memberStatus }
    };
  } catch(e) {
    return "Error: " + e.toString();
  } finally {
    if (lockAcquired) lock.releaseLock();
  }
}

function updateMember(memberDataStr) {
  requirePermission_('members.manage', 'ไม่มีสิทธิ์แก้ไขสมาชิก');
  var lock = LockService.getScriptLock();
  var lockAcquired = false;
  try {
    lock.waitLock(30000);
    lockAcquired = true;
    var ss = getDatabaseSpreadsheet_();
    var memberSheet = getSheetOrThrow(ss, "Members");
    var mData = validateMemberPayloadForWrite_(JSON.parse(memberDataStr));

    var row = findRowByExactValue(memberSheet, 1, mData.id);

    if (row) {
      var previousMemberName = String(memberSheet.getRange(row, 2).getValue() || "").trim();

      memberSheet.getRange(row, 2, 1, 2).setValues([[
        mData.name,
        mData.status
      ]]);
      sortMembersSheetByMemberId_(memberSheet);
      var affectedContracts = syncLoanBorrowerNameForMember_(ss, mData.id, mData.name || "");
      syncGuarantorReferencesForMember_(ss, mData.id, previousMemberName, mData.name || "");
      if (affectedContracts.length > 0) {
        syncPaymentSearchIndexForContracts_(ss, affectedContracts);
      }
      appendAuditLogSafe_(ss, {
        action: 'UPDATE_MEMBER',
        entityType: 'MEMBER',
        entityId: mData.id,
        referenceNo: mData.id,
        before: { id: mData.id, previousName: previousMemberName },
        after: { id: mData.id, name: mData.name, status: mData.status },
        details: 'แก้ไขข้อมูลสมาชิกสำเร็จ'
      });
      clearAppCache_();
      return "Success";
    }
    return "Error: ไม่พบรหัสสมาชิก";
  } catch(e) {
    return "Error: " + e.toString();
  } finally {
    if (lockAcquired) lock.releaseLock();
  }
}

function deleteMember(memberId) {
  requirePermission_('members.manage', 'ไม่มีสิทธิ์ลบสมาชิก');

  var lock = LockService.getScriptLock();
  var lockAcquired = false;
  try {
    lock.waitLock(30000);
    lockAcquired = true;
    var ss = getDatabaseSpreadsheet_();
    var memberSheet = getSheetOrThrow(ss, "Members");
    var loanSheet = getSheetOrThrow(ss, "Loans");
    var normalizedMemberId = String(memberId || '').trim();
    var row = findRowByExactValue(memberSheet, 1, normalizedMemberId);

    if (!row) return "Error: ไม่พบรหัสสมาชิก";

    if (loanSheet.getLastRow() > 1) {
      var loanRows = loanSheet.getRange(2, 1, loanSheet.getLastRow() - 1, Math.max(loanSheet.getLastColumn(), 12)).getValues();
      for (var i = 0; i < loanRows.length; i++) {
        var loanRow = loanRows[i];
        if (String(loanRow[0] || '').trim() === normalizedMemberId) {
          return "Error: สมาชิกนี้มีสัญญากู้ในระบบอยู่ ไม่อนุญาตให้ลบ";
        }
        if (guarantorReferenceMatchesMember_(loanRow[10], normalizedMemberId, '') || guarantorReferenceMatchesMember_(loanRow[11], normalizedMemberId, '')) {
          return "Error: สมาชิกนี้ถูกอ้างอิงเป็นผู้ค้ำประกันอยู่ ไม่อนุญาตให้ลบ";
        }
      }
    }

    if (hasActiveTransactionForMember_(ss, normalizedMemberId)) {
      return "Error: สมาชิกนี้มีประวัติธุรกรรมแล้ว ไม่อนุญาตให้ลบ";
    }

    memberSheet.deleteRow(row);
    sortMembersSheetByMemberId_(memberSheet);
    appendAuditLogSafe_(ss, {
      action: 'DELETE_MEMBER',
      entityType: 'MEMBER',
      entityId: normalizedMemberId,
      referenceNo: normalizedMemberId,
      reason: 'ลบสมาชิกที่ไม่มีสัญญาและไม่มีธุรกรรม',
      details: 'ลบข้อมูลสมาชิกสำเร็จ'
    });
    clearAppCache_();
    logSystemActionFast(ss, "ลบข้อมูลสมาชิก", "รหัส: " + normalizedMemberId);
    return "Success";
  } catch(e) {
    return "Error: " + e.toString();
  } finally {
    if (lockAcquired) lock.releaseLock();
  }
}

// 5. ระบบความปลอดภัยและการตั้งค่า (SECURITY & SETTINGS)
// 5. ระบบความปลอดภัยและการตั้งค่า (SECURITY & SETTINGS)
// ==========================================


function verifyLoginPin(username, pin) {
  var ss = getDatabaseSpreadsheet_();
  var usersSheet = ensureUsersSheet_(ss);
  var normalizedUsername = normalizeUsername_(username);

  if (!normalizedUsername) {
    return { status: 'Error', message: 'กรุณากรอกชื่อผู้ใช้' };
  }

  var normalizedPin = normalizePinInput_(pin);
  if (!isValidSixDigitPin_(normalizedPin)) {
    return { status: 'Error', message: 'กรุณากรอก PIN 6 หลัก' };
  }

  var lockoutState = getLoginLockoutState_(normalizedUsername);
  if (lockoutState.locked) {
    return { status: 'Locked', message: 'พยายามเข้าสู่ระบบผิดเกินกำหนด กรุณารอ 15 นาทีแล้วลองใหม่อีกครั้ง' };
  }

  var userRecord = findUserByUsernameInternal_(usersSheet, normalizedUsername);
  if (!userRecord || !userRecord.pinHash) {
    var failedStateNotFound = registerFailedLoginAttempt_(normalizedUsername);
    return {
      status: failedStateNotFound.locked ? 'Locked' : 'Error',
      message: failedStateNotFound.locked
        ? 'พยายามเข้าสู่ระบบผิดเกินกำหนด กรุณารอ 15 นาทีแล้วลองใหม่อีกครั้ง'
        : 'ชื่อผู้ใช้ หรือ PIN ไม่ถูกต้อง'
    };
  }

  var normalizedStatus = normalizeUserStatus_(userRecord.status || USER_STATUS_PENDING);
  if (normalizedStatus !== USER_STATUS_ACTIVE) {
    return {
      status: 'Error',
      message: normalizedStatus === USER_STATUS_PENDING
        ? 'บัญชีของคุณอยู่ระหว่างรออนุมัติจากผู้ดูแลระบบ'
        : 'บัญชีผู้ใช้นี้ถูกระงับการใช้งาน กรุณาติดต่อผู้ดูแลระบบ'
    };
  }

  if (!String(userRecord.emailVerifiedAt || '').trim()) {
    return { status: 'Error', message: 'บัญชีนี้ยังไม่ได้ยืนยันอีเมล กรุณายืนยันอีเมลก่อนเข้าสู่ระบบ' };
  }

  var pinMatched = verifyPasswordAgainstStoredHash_(normalizedPin, userRecord.pinHash);
  if (!pinMatched) {
    var failedState = registerFailedLoginAttempt_(userRecord.username || normalizedUsername);
    return {
      status: failedState.locked ? 'Locked' : 'Error',
      message: failedState.locked
        ? 'พยายามเข้าสู่ระบบผิดเกินกำหนด กรุณารอ 15 นาทีแล้วลองใหม่อีกครั้ง'
        : 'ชื่อผู้ใช้ หรือ PIN ไม่ถูกต้อง'
    };
  }

  var loginAt = getThaiDate(new Date()).dateTime;
  updateUserCellsByRecord_(usersSheet, userRecord, {
    lastLoginAt: loginAt,
    updatedAt: loginAt
  });

  clearFailedLoginAttempts_(userRecord.username || normalizedUsername);
  invalidateUserAuthorizationCache_(userRecord.username || normalizedUsername);
  var authenticatedSession = createAuthenticatedSession_({
    userId: userRecord.userId,
    username: userRecord.username,
    fullName: userRecord.fullName,
    role: userRecord.role
  });
  var sessionToken = createApiSessionToken_(authenticatedSession);
  logSystemActionFast(ss, 'เข้าสู่ระบบ', 'ผู้ใช้งาน: ' + (userRecord.username || '-'));
  return {
    status: 'Success',
    username: userRecord.username,
    fullName: userRecord.fullName || userRecord.username,
    role: userRecord.role || 'staff',
    permissions: getEffectivePermissionsForUserRecord_(userRecord),
    permissionCatalog: getPermissionCatalog_(),
    sessionToken: sessionToken,
    backendMode: 'gas-web-app'
  };
}

function verifyLogin(username, password) {
  return verifyLoginPin(username, password);
}


function changePin(currentPin, newPin, confirmPin) {
  var session = requireAuthenticatedSession_();

  var lock = LockService.getScriptLock();
  var lockAcquired = false;
  try {
    lock.waitLock(30000);
    lockAcquired = true;

    var ss = getDatabaseSpreadsheet_();
    var usersSheet = ensureUsersSheet_(ss);
    var userRecord = findUserByUsernameInternal_(usersSheet, session.username);

    if (!userRecord) return 'Error: ไม่พบบัญชีผู้ใช้งานในระบบ';
    if (!verifyPasswordAgainstStoredHash_(normalizePinInput_(currentPin), userRecord.pinHash)) return 'InvalidOldPassword';

    var normalizedNewPin = normalizePinInput_(newPin);
    var normalizedConfirmPin = normalizePinInput_(confirmPin);
    if (!isValidSixDigitPin_(normalizedNewPin)) {
      return 'Error: PIN ใหม่ต้องเป็นตัวเลข 6 หลัก';
    }
    if (normalizedNewPin !== normalizedConfirmPin) {
      return 'Error: ยืนยัน PIN ใหม่ไม่ตรงกัน';
    }
    if (verifyPasswordAgainstStoredHash_(normalizedNewPin, userRecord.pinHash)) {
      return 'Error: PIN ใหม่ต้องไม่ซ้ำกับ PIN เดิม';
    }

    var pinUniqueKey = ensurePinUniqueAcrossUsers_(usersSheet, normalizedNewPin, userRecord.userId);
    var nowText = getThaiDate(new Date()).dateTime;
    updateUserCellsByRecord_(usersSheet, userRecord, {
      pinHash: createPasswordHashRecord_(normalizedNewPin),
      pinUniqueKey: pinUniqueKey,
      pinUpdatedAt: nowText,
      updatedAt: nowText,
      resetOtpHash: '',
      resetOtpExpireAt: '',
      resetOtpRequestedAt: '',
      resetOtpUsedAt: ''
    });
    invalidateUserAuthorizationCache_(userRecord.username);
    clearFailedLoginAttempts_(userRecord.username);
    createAuthenticatedSession_({
      userId: userRecord.userId,
      username: userRecord.username,
      fullName: userRecord.fullName,
      role: userRecord.role
    });
    logSystemActionFast(ss, 'เปลี่ยน PIN', 'ผู้ใช้งาน: ' + (userRecord.username || '-'));
    return 'Success';
  } catch(e) {
    return 'Error: ' + e.toString();
  } finally {
    if (lockAcquired) lock.releaseLock();
  }
}

function changePassword(oldPassword, newPassword) {
  return changePin(oldPassword, newPassword, newPassword);
}


function registerUser(registerPayloadStr) {
  var lock = LockService.getScriptLock();
  var lockAcquired = false;
  try {
    lock.waitLock(30000);
    lockAcquired = true;

    var payload = typeof registerPayloadStr === 'string' ? JSON.parse(registerPayloadStr) : (registerPayloadStr || {});
    var prefix = normalizePersonPrefix_(payload.prefix || payload.title);
    var firstName = normalizePersonNamePart_(payload.firstName || '');
    var lastName = normalizePersonNamePart_(payload.lastName || '');
    var fullName = buildFullNameFromParts_(prefix, firstName, lastName);
    var username = normalizeUsername_(payload.username);
    var email = normalizeEmail_(payload.email);
    var pin = normalizePinInput_(payload.pin);
    var confirmPin = normalizePinInput_(payload.confirmPin);

    if (!fullName) return { status: 'Error', message: 'กรุณากรอกชื่อ-สกุล' };
    if (!username || username.length < 4) return { status: 'Error', message: 'ชื่อผู้ใช้ต้องยาวอย่างน้อย 4 ตัวอักษร' };
    if (!/^[a-zA-Z0-9._-]+$/.test(username)) return { status: 'Error', message: 'ชื่อผู้ใช้ใช้ได้เฉพาะภาษาอังกฤษ ตัวเลข จุด ขีด และขีดล่าง' };
    if (!email || !isValidEmail_(email)) return { status: 'Error', message: 'กรุณากรอกอีเมลให้ถูกต้อง' };
    if (!isValidSixDigitPin_(pin)) return { status: 'Error', message: 'PIN ต้องเป็นตัวเลข 6 หลัก' };
    if (pin !== confirmPin) return { status: 'Error', message: 'ยืนยัน PIN ไม่ตรงกัน' };

    var ss = getDatabaseSpreadsheet_();
    var usersSheet = ensureUsersSheet_(ss);

    if (findUserByUsernameInternal_(usersSheet, username)) {
      return { status: 'Error', message: 'ชื่อผู้ใช้นี้ถูกใช้งานแล้ว' };
    }
    var emailOwner = findUserByIdentifierInternal_(usersSheet, email);
    if (emailOwner && emailOwner.email === email) {
      return { status: 'Error', message: 'อีเมลนี้ถูกใช้งานแล้ว' };
    }

    var pinUniqueKey = ensurePinUniqueAcrossUsers_(usersSheet, pin, '');
    var emailVerifyOtpCode = generateSixDigitOtp_();

    var nowText = getThaiDate(new Date()).dateTime;
    var emailVerifyExpireAt = getThaiDate(addMinutesToDate_(new Date(), EMAIL_VERIFICATION_OTP_TTL_MINUTES)).dateTime;
    appendRowsSafely_(usersSheet, [[
      Utilities.getUuid(),
      username,
      fullName,
      email,
      '',
      'staff',
      USER_STATUS_PENDING,
      nowText,
      nowText,
      '',
      '',
      '',
      '',
      '',
      '',
      prefix,
      firstName,
      lastName,
      '',
      createPasswordHashRecord_(pin),
      pinUniqueKey,
      nowText,
      createPasswordHashRecord_(emailVerifyOtpCode),
      emailVerifyExpireAt,
      nowText
    ]], 'เพิ่มผู้ใช้ใหม่');

    MailApp.sendEmail({
      to: email,
      subject: 'OTP ยืนยันอีเมล - ระบบจัดการเงินกู้ บ้านพิตำ',
      body: [
        'เรียน ' + fullName,
        '',
        'รหัส OTP สำหรับยืนยันอีเมลคือ: ' + emailVerifyOtpCode,
        'รหัสนี้มีอายุ ' + EMAIL_VERIFICATION_OTP_TTL_MINUTES + ' นาที',
        '',
        'เมื่อยืนยันอีเมลแล้ว บัญชีจะพร้อมรอผู้ดูแลอนุมัติการใช้งาน',
        '',
        'ระบบจัดการเงินกู้ บ้านพิตำ'
      ].join('\n')
    });

    clearAppCache_();
    logSystemActionFast(ss, 'สมัครใช้งาน', 'ผู้ใช้งานใหม่: ' + username + ' | Email: ' + email, username);
    return {
      status: 'Success',
      message: 'สมัครใช้งานสำเร็จ กรุณากรอก OTP ที่ส่งไปยังอีเมลเพื่อยืนยันอีเมลก่อนรอผู้ดูแลอนุมัติ',
      requiresEmailVerification: true,
      identifier: username
    };
  } catch(e) {
    return { status: 'Error', message: e.toString() };
  } finally {
    if (lockAcquired) lock.releaseLock();
  }
}

function requestEmailVerificationOtp(identifier) {
  var lock = LockService.getScriptLock();
  var lockAcquired = false;
  try {
    lock.waitLock(30000);
    lockAcquired = true;

    var normalizedIdentifier = String(identifier || '').trim();
    if (!normalizedIdentifier) return { status: 'Error', message: 'กรุณากรอกชื่อผู้ใช้หรืออีเมล' };

    var ss = getDatabaseSpreadsheet_();
    var usersSheet = ensureUsersSheet_(ss);
    var userRecord = findUserByIdentifierInternal_(usersSheet, normalizedIdentifier);
    if (!userRecord || !userRecord.email) return { status: 'Error', message: 'ไม่พบบัญชีผู้ใช้งาน' };
    if (String(userRecord.emailVerifiedAt || '').trim()) return { status: 'Success', message: 'บัญชีนี้ยืนยันอีเมลแล้ว' };

    var otpCode = generateSixDigitOtp_();
    var nowText = getThaiDate(new Date()).dateTime;
    var expireAtText = getThaiDate(addMinutesToDate_(new Date(), EMAIL_VERIFICATION_OTP_TTL_MINUTES)).dateTime;

    updateUserCellsByRecord_(usersSheet, userRecord, {
      emailVerifyOtpHash: createPasswordHashRecord_(otpCode),
      emailVerifyOtpExpireAt: expireAtText,
      emailVerifyOtpRequestedAt: nowText,
      updatedAt: nowText
    });

    MailApp.sendEmail({
      to: userRecord.email,
      subject: 'OTP ยืนยันอีเมล - ระบบจัดการเงินกู้ บ้านพิตำ',
      body: [
        'เรียน ' + (userRecord.fullName || userRecord.username || 'ผู้ใช้งาน'),
        '',
        'รหัส OTP สำหรับยืนยันอีเมลคือ: ' + otpCode,
        'รหัสนี้มีอายุ ' + EMAIL_VERIFICATION_OTP_TTL_MINUTES + ' นาที',
        '',
        'ระบบจัดการเงินกู้ บ้านพิตำ'
      ].join('\n')
    });

    clearAppCache_();
    return { status: 'Success', message: 'ส่ง OTP ยืนยันอีเมลแล้ว' };
  } catch (e) {
    return { status: 'Error', message: 'ส่ง OTP ยืนยันอีเมลไม่สำเร็จ: ' + e.toString() };
  } finally {
    if (lockAcquired) lock.releaseLock();
  }
}

function verifyEmailWithOtp(payloadStr) {
  var lock = LockService.getScriptLock();
  var lockAcquired = false;
  try {
    lock.waitLock(30000);
    lockAcquired = true;

    var payload = typeof payloadStr === 'string' ? JSON.parse(payloadStr) : (payloadStr || {});
    var identifier = String(payload.identifier || '').trim();
    var otp = String(payload.otp || '').trim();
    if (!identifier) return { status: 'Error', message: 'กรุณากรอกชื่อผู้ใช้หรืออีเมล' };
    if (!otp) return { status: 'Error', message: 'กรุณากรอก OTP' };

    var ss = getDatabaseSpreadsheet_();
    var usersSheet = ensureUsersSheet_(ss);
    var userRecord = findUserByIdentifierInternal_(usersSheet, identifier);
    if (!userRecord) return { status: 'Error', message: 'ไม่พบบัญชีผู้ใช้งาน' };
    if (String(userRecord.emailVerifiedAt || '').trim()) return { status: 'Success', message: 'บัญชีนี้ยืนยันอีเมลแล้ว' };
    if (!userRecord.emailVerifyOtpHash || !userRecord.emailVerifyOtpExpireAt) return { status: 'Error', message: 'ไม่พบ OTP ที่ใช้งานได้ กรุณาขอ OTP ใหม่' };

    var expireParts = parseThaiDateParts_(userRecord.emailVerifyOtpExpireAt);
    if (!expireParts || !expireParts.nativeDate || expireParts.nativeDate.getTime() < new Date().getTime()) {
      return { status: 'Error', message: 'OTP หมดอายุแล้ว กรุณาขอ OTP ใหม่' };
    }

    if (!verifyPasswordAgainstStoredHash_(otp, userRecord.emailVerifyOtpHash)) {
      return { status: 'Error', message: 'OTP ไม่ถูกต้อง' };
    }

    var nowText = getThaiDate(new Date()).dateTime;
    updateUserCellsByRecord_(usersSheet, userRecord, {
      emailVerifiedAt: nowText,
      emailVerifyOtpHash: '',
      emailVerifyOtpExpireAt: '',
      emailVerifyOtpRequestedAt: '',
      updatedAt: nowText
    });
    clearAppCache_();
    logSystemActionFast(ss, 'ยืนยันอีเมลผู้ใช้', 'ผู้ใช้งาน: ' + (userRecord.username || '-'), userRecord.username || 'system');
    return { status: 'Success', message: 'ยืนยันอีเมลสำเร็จ กรุณารอผู้ดูแลอนุมัติบัญชี' };
  } catch (e) {
    return { status: 'Error', message: e.toString() };
  } finally {
    if (lockAcquired) lock.releaseLock();
  }
}


function requestPinResetOtp(identifier) {
  var lock = LockService.getScriptLock();
  var lockAcquired = false;
  try {
    lock.waitLock(30000);
    lockAcquired = true;

    var normalizedIdentifier = String(identifier || '').trim();
    if (!normalizedIdentifier) {
      return { status: 'Error', message: 'กรุณากรอกชื่อผู้ใช้หรืออีเมล' };
    }

    var requestCooldownSeconds = getPasswordResetRequestThrottleRemainingSeconds_(normalizedIdentifier);
    if (requestCooldownSeconds > 0) {
      return { status: 'Error', message: 'กรุณารอ ' + requestCooldownSeconds + ' วินาทีก่อนขอ OTP ใหม่อีกครั้ง' };
    }

    var ss = getDatabaseSpreadsheet_();
    var usersSheet = ensureUsersSheet_(ss);
    var userRecord = findUserByIdentifierInternal_(usersSheet, normalizedIdentifier);

    if (!userRecord || !userRecord.email || normalizeUserStatus_(userRecord.status || USER_STATUS_PENDING) !== USER_STATUS_ACTIVE) {
      setPasswordResetRequestThrottle_(normalizedIdentifier);
      return {
        status: 'Success',
        message: 'ถ้าพบบัญชีที่ตรงกัน ระบบจะส่งรหัส OTP ไปยังอีเมลที่ยืนยันไว้'
      };
    }

    if (!String(userRecord.emailVerifiedAt || '').trim()) {
      setPasswordResetRequestThrottle_(normalizedIdentifier);
      return { status: 'Error', message: 'บัญชีนี้ยังไม่ได้ยืนยันอีเมล กรุณายืนยันอีเมลก่อน' };
    }

    var userCooldownSeconds = Math.max(
      getPasswordResetRequestThrottleRemainingSeconds_(userRecord.username),
      getPasswordResetRequestThrottleRemainingSeconds_(userRecord.email)
    );
    if (userCooldownSeconds > 0) {
      setPasswordResetRequestThrottle_(normalizedIdentifier);
      return { status: 'Error', message: 'กรุณารอ ' + userCooldownSeconds + ' วินาทีก่อนขอ OTP ใหม่อีกครั้ง' };
    }

    var now = new Date();
    var nowText = getThaiDate(now).dateTime;
    var expireAtText = getThaiDate(addMinutesToDate_(now, PASSWORD_RESET_OTP_TTL_MINUTES)).dateTime;
    var otpCode = generateSixDigitOtp_();

    updateUserCellsByRecord_(usersSheet, userRecord, {
      resetOtpHash: createPasswordHashRecord_(otpCode),
      resetOtpExpireAt: expireAtText,
      resetOtpRequestedAt: nowText,
      resetOtpUsedAt: '',
      updatedAt: nowText
    });

    MailApp.sendEmail({
      to: userRecord.email,
      subject: 'OTP ตั้ง PIN ใหม่ - ระบบจัดการเงินกู้ บ้านพิตำ',
      body: buildPasswordResetEmailBody_(userRecord.fullName || userRecord.username, otpCode)
    });

    setPasswordResetRequestThrottle_(normalizedIdentifier);
    setPasswordResetRequestThrottle_(userRecord.username);
    setPasswordResetRequestThrottle_(userRecord.email);
    clearPasswordResetVerifyFailures_(normalizedIdentifier);
    clearPasswordResetVerifyFailures_(userRecord.username);
    clearPasswordResetVerifyFailures_(userRecord.email);

    clearAppCache_();
    logSystemActionFast(ss, 'ขอรหัส OTP รีเซ็ต PIN', 'ผู้ใช้งาน: ' + (userRecord.username || '-') + ' | Email: ' + maskEmailForDisplay_(userRecord.email), userRecord.username);
    return {
      status: 'Success',
      message: 'ส่งรหัส OTP ไปยังอีเมล ' + maskEmailForDisplay_(userRecord.email) + ' แล้ว'
    };
  } catch(e) {
    return { status: 'Error', message: 'ส่ง OTP ไม่สำเร็จ: ' + e.toString() };
  } finally {
    if (lockAcquired) lock.releaseLock();
  }
}

function requestPasswordResetOtp(identifier) {
  return requestPinResetOtp(identifier);
}


function resetPinWithOtp(resetPayloadStr) {
  var lock = LockService.getScriptLock();
  var lockAcquired = false;
  try {
    lock.waitLock(30000);
    lockAcquired = true;

    var payload = typeof resetPayloadStr === 'string' ? JSON.parse(resetPayloadStr) : (resetPayloadStr || {});
    var identifier = String(payload.identifier || '').trim();
    var otpCode = String(payload.otp || '').trim();
    var newPin = normalizePinInput_(payload.newPin);
    var confirmPin = normalizePinInput_(payload.confirmPin);

    if (!identifier) return { status: 'Error', message: 'กรุณากรอกชื่อผู้ใช้หรืออีเมล' };
    if (!otpCode) return { status: 'Error', message: 'กรุณากรอกรหัส OTP' };
    if (!isValidSixDigitPin_(newPin)) return { status: 'Error', message: 'PIN ใหม่ต้องเป็นตัวเลข 6 หลัก' };
    if (newPin !== confirmPin) return { status: 'Error', message: 'ยืนยัน PIN ใหม่ไม่ตรงกัน' };

    var verifyLockSeconds = getPasswordResetVerifyLockRemainingSeconds_(identifier);
    if (verifyLockSeconds > 0) {
      return { status: 'Error', message: 'กรอกรหัส OTP ผิดเกินกำหนด กรุณารอ ' + verifyLockSeconds + ' วินาทีแล้วลองใหม่อีกครั้ง' };
    }

    var ss = getDatabaseSpreadsheet_();
    var usersSheet = ensureUsersSheet_(ss);
    var userRecord = findUserByIdentifierInternal_(usersSheet, identifier);
    if (!userRecord) return { status: 'Error', message: 'ไม่พบบัญชีผู้ใช้งานที่ตรงกับข้อมูลที่กรอก' };
    if (!String(userRecord.emailVerifiedAt || '').trim()) return { status: 'Error', message: 'บัญชีนี้ยังไม่ได้ยืนยันอีเมล' };

    var userVerifyLockSeconds = Math.max(
      getPasswordResetVerifyLockRemainingSeconds_(userRecord.username),
      getPasswordResetVerifyLockRemainingSeconds_(userRecord.email)
    );
    if (userVerifyLockSeconds > 0) {
      return { status: 'Error', message: 'กรอกรหัส OTP ผิดเกินกำหนด กรุณารอ ' + userVerifyLockSeconds + ' วินาทีแล้วลองใหม่อีกครั้ง' };
    }

    if (!userRecord.resetOtpHash || !userRecord.resetOtpExpireAt) return { status: 'Error', message: 'ไม่พบ OTP ที่ยังใช้งานได้ กรุณาขอรหัสใหม่อีกครั้ง' };

    var expireParts = parseThaiDateParts_(userRecord.resetOtpExpireAt);
    if (!expireParts || !expireParts.nativeDate || expireParts.nativeDate.getTime() < new Date().getTime()) {
      return { status: 'Error', message: 'OTP หมดอายุแล้ว กรุณาขอรหัสใหม่อีกครั้ง' };
    }

    if (!verifyPasswordAgainstStoredHash_(otpCode, userRecord.resetOtpHash)) {
      var verifyFailure = registerPasswordResetVerifyFailure_(identifier);
      var userVerifyFailure = registerPasswordResetVerifyFailure_(userRecord.username);
      registerPasswordResetVerifyFailure_(userRecord.email);
      if (verifyFailure.locked || userVerifyFailure.locked) {
        return { status: 'Error', message: 'กรอกรหัส OTP ผิดเกินกำหนด กรุณารอ 15 นาทีแล้วลองใหม่อีกครั้ง' };
      }
      return { status: 'Error', message: 'OTP ไม่ถูกต้อง' };
    }

    if (verifyPasswordAgainstStoredHash_(newPin, userRecord.pinHash)) {
      return { status: 'Error', message: 'PIN ใหม่ต้องไม่ซ้ำกับ PIN เดิม' };
    }

    var pinUniqueKey = ensurePinUniqueAcrossUsers_(usersSheet, newPin, userRecord.userId);
    var nowText = getThaiDate(new Date()).dateTime;
    updateUserCellsByRecord_(usersSheet, userRecord, {
      pinHash: createPasswordHashRecord_(newPin),
      pinUniqueKey: pinUniqueKey,
      pinUpdatedAt: nowText,
      updatedAt: nowText,
      resetOtpHash: '',
      resetOtpExpireAt: '',
      resetOtpRequestedAt: '',
      resetOtpUsedAt: nowText
    });

    invalidateUserAuthorizationCache_(userRecord.username);
    clearFailedLoginAttempts_(userRecord.username);
    clearPasswordResetVerifyFailures_(identifier);
    clearPasswordResetVerifyFailures_(userRecord.username);
    clearPasswordResetVerifyFailures_(userRecord.email);
    clearAppCache_();
    logSystemActionFast(ss, 'รีเซ็ต PIN', 'ผู้ใช้งาน: ' + (userRecord.username || '-'), userRecord.username);
    return { status: 'Success', message: 'ตั้ง PIN ใหม่เรียบร้อยแล้ว กรุณาเข้าสู่ระบบอีกครั้ง' };
  } catch(e) {
    return { status: 'Error', message: e.toString() };
  } finally {
    if (lockAcquired) lock.releaseLock();
  }
}

function resetPasswordWithOtp(resetPayloadStr) {
  var payload = typeof resetPayloadStr === 'string' ? JSON.parse(resetPayloadStr) : (resetPayloadStr || {});
  payload.newPin = payload.newPin || payload.newPassword;
  payload.confirmPin = payload.confirmPin || payload.confirmPassword;
  return resetPinWithOtp(payload);
}

function emergencyRestoreAdminAccess(recoveryKey, newPin) {
  var providedKey = String(recoveryKey || '').trim();
  var configuredKey = String(PropertiesService.getScriptProperties().getProperty('ADMIN_EMERGENCY_RECOVERY_KEY') || '').trim();
  if (!configuredKey || configuredKey.length < 16) {
    return {
      status: 'Error',
      message: 'ยังไม่ได้ตั้งค่า Script Property: ADMIN_EMERGENCY_RECOVERY_KEY (อย่างน้อย 16 ตัวอักษร)'
    };
  }
  if (!providedKey || providedKey !== configuredKey) {
    return { status: 'Error', message: 'Recovery key ไม่ถูกต้อง' };
  }

  var normalizedPin = normalizePinInput_(newPin);
  if (!isValidSixDigitPin_(normalizedPin)) {
    return { status: 'Error', message: 'PIN ใหม่ต้องเป็นตัวเลข 6 หลัก' };
  }

  var lock = LockService.getScriptLock();
  var lockAcquired = false;
  try {
    lock.waitLock(30000);
    lockAcquired = true;

    var ss = getDatabaseSpreadsheet_();
    var usersSheet = ensureUsersSheet_(ss);
    var adminUser = findUserByUsernameInternal_(usersSheet, 'admin');
    if (!adminUser) {
      migrateLegacyAdminUserToUsersSheet_(ss, usersSheet);
      adminUser = findUserByUsernameInternal_(usersSheet, 'admin');
    }
    if (!adminUser) {
      return { status: 'Error', message: 'ไม่พบบัญชี admin และไม่สามารถสร้างใหม่ได้' };
    }

    var pinUniqueKey = ensurePinUniqueAcrossUsers_(usersSheet, normalizedPin, adminUser.userId);
    var nowText = getThaiDate(new Date()).dateTime;
    updateUserCellsByRecord_(usersSheet, adminUser, {
      fullName: String(adminUser.fullName || 'ผู้ดูแลระบบ').trim() || 'ผู้ดูแลระบบ',
      role: 'admin',
      status: USER_STATUS_ACTIVE,
      emailVerifiedAt: String(adminUser.emailVerifiedAt || nowText).trim() || nowText,
      pinHash: createPasswordHashRecord_(normalizedPin),
      pinUniqueKey: pinUniqueKey,
      pinUpdatedAt: nowText,
      updatedAt: nowText,
      resetOtpHash: '',
      resetOtpExpireAt: '',
      resetOtpRequestedAt: '',
      resetOtpUsedAt: '',
      emailVerifyOtpHash: '',
      emailVerifyOtpExpireAt: '',
      emailVerifyOtpRequestedAt: ''
    });

    clearFailedLoginAttempts_('admin');
    invalidateUserAuthorizationCache_('admin');
    clearAuthenticatedSession_();
    clearAppCache_();
    logSystemActionFast(ss, 'กู้บัญชีแอดมินฉุกเฉิน', 'รีเซ็ต PIN และปลดล็อกบัญชี admin ผ่าน emergency recovery', 'system');

    return {
      status: 'Success',
      message: 'กู้บัญชี admin สำเร็จ กรุณาเข้าสู่ระบบด้วย PIN ใหม่',
      username: 'admin'
    };
  } catch (e) {
    return { status: 'Error', message: 'กู้บัญชี admin ไม่สำเร็จ: ' + e.toString() };
  } finally {
    if (lockAcquired) lock.releaseLock();
  }
}



function getUserAdminList() {
  requirePermission_('users.manage', 'ไม่มีสิทธิ์จัดการผู้ใช้งาน');
  var cacheKey = 'userAdminList';
  var cached = getJsonCache_(cacheKey);
  if (cached && Array.isArray(cached.users)) {
    return { status: 'Success', users: cached.users };
  }
  var ss = getDatabaseSpreadsheet_();
  var usersSheet = ensureUsersSheet_(ss);
  var users = getAllUserRecords_(usersSheet).map(sanitizeUserRecordForClient_);
  users.sort(function(a, b) {
    var statusOrder = {};
    statusOrder[USER_STATUS_PENDING] = 0;
    statusOrder[USER_STATUS_ACTIVE] = 1;
    statusOrder[USER_STATUS_SUSPENDED] = 2;
    var aStatus = normalizeUserStatus_(a.status);
    var bStatus = normalizeUserStatus_(b.status);
    var diff = (statusOrder[aStatus] !== undefined ? statusOrder[aStatus] : 99) - (statusOrder[bStatus] !== undefined ? statusOrder[bStatus] : 99);
    if (diff !== 0) return diff;
    return String(a.username || '').localeCompare(String(b.username || ''));
  });
  var payload = { status: 'Success', users: users };
  putJsonCache_(cacheKey, payload, 30);
  return payload;
}

function adminUpdateUserAccount(payloadStr) {
  requirePermission_('users.manage', 'ไม่มีสิทธิ์จัดการผู้ใช้งาน');
  var lock = LockService.getScriptLock();
  var lockAcquired = false;
  try {
    lock.waitLock(10000);
    lockAcquired = true;

    var payload = typeof payloadStr === 'string' ? JSON.parse(payloadStr) : (payloadStr || {});
    var targetUserId = String(payload.userId || '').trim();
    if (!targetUserId) return { status: 'Error', message: 'ไม่พบรหัสผู้ใช้ที่ต้องการแก้ไข' };

    var ss = getDatabaseSpreadsheet_();
    var usersSheet = ensureUsersSheet_(ss);
    var targetUser = findUserByIdInternal_(usersSheet, targetUserId);
    if (!targetUser) return { status: 'Error', message: 'ไม่พบบัญชีผู้ใช้ที่ต้องการแก้ไข' };

    var sessionData = requireAdminSession_();
    var actingUser = findUserByUsernameInternal_(usersSheet, sessionData.username);
    var nextRole = payload.role !== undefined ? normalizeUserRole_(payload.role) : normalizeUserRole_(targetUser.role);
    var nextStatus = payload.status !== undefined ? normalizeUserStatus_(payload.status) : normalizeUserStatus_(targetUser.status);
    var nextFullName = payload.fullName !== undefined ? String(payload.fullName || '').replace(/\s+/g, ' ').trim() : String(targetUser.fullName || '');
    var nextPermissionsJson = payload.permissionsJson !== undefined
      ? normalizePermissionsJson_(payload.permissionsJson)
      : normalizePermissionsJson_(targetUser.permissionsJson || '');

    if (!nextFullName) return { status: 'Error', message: 'กรุณาระบุชื่อ-สกุลของผู้ใช้' };

    if (actingUser && String(actingUser.userId || '').trim() === targetUserId) {
      if (nextStatus !== USER_STATUS_ACTIVE) {
        return { status: 'Error', message: 'ไม่อนุญาตให้ระงับบัญชีที่กำลังใช้งานอยู่ของตนเอง' };
      }
      if (normalizeUserRole_(targetUser.role) === 'admin' && nextRole !== 'admin') {
        return { status: 'Error', message: 'ไม่อนุญาตให้ลดสิทธิ์ผู้ดูแลของบัญชีที่กำลังใช้งานอยู่' };
      }
    }

    if (normalizeUserRole_(targetUser.role) === 'admin' && (nextRole !== 'admin' || nextStatus !== USER_STATUS_ACTIVE)) {
      if (countActiveAdmins_(usersSheet, targetUserId) <= 0) {
        return { status: 'Error', message: 'ต้องมีผู้ดูแลระบบที่ใช้งานได้อย่างน้อย 1 บัญชีเสมอ' };
      }
    }

    var nowText = getThaiDate(new Date()).dateTime;
    updateUserCellsByRecord_(usersSheet, targetUser, {
      fullName: nextFullName,
      role: nextRole,
      status: nextStatus,
      updatedAt: nowText,
      permissionsJson: nextPermissionsJson
    });
    invalidateUserAuthorizationCache_(targetUser.username);
    var updatedTargetUser = findUserByIdInternal_(usersSheet, targetUserId) || targetUser;

    appendAuditLogSafe_(ss, {
      action: 'ADMIN_UPDATE_USER',
      entityType: 'USER',
      entityId: String(targetUser.userId || '').trim(),
      referenceNo: String(targetUser.username || '').trim(),
      before: sanitizeUserRecordForClient_(targetUser),
      after: sanitizeUserRecordForClient_(updatedTargetUser),
      details: 'แก้ไขสิทธิ์/สถานะผู้ใช้งานโดยผู้ดูแลระบบ'
    });
    clearAppCache_();
    logSystemActionFast(ss, 'จัดการผู้ใช้งาน', 'แก้ไขผู้ใช้: ' + (targetUser.username || '-') + ' | role=' + nextRole + ' | status=' + nextStatus, sessionData.username || 'admin');
    return {
      status: 'Success',
      message: 'บันทึกข้อมูลผู้ใช้เรียบร้อยแล้ว',
      user: sanitizeUserRecordForClient_(updatedTargetUser)
    };
  } catch (e) {
    return { status: 'Error', message: e.toString() };
  } finally {
    if (lockAcquired) lock.releaseLock();
  }
}

function logSystemActionFast(ss, action, details, actorOverride) {
  try {
    var logSheet = ss.getSheetByName("SystemLogs") || createSheetSafely_(ss, "SystemLogs", 2, 4, 'เตรียมชีต SystemLogs');
    if (logSheet.getLastRow() === 0) {
      logSheet.getRange(1, 1, 1, 4)
        .setValues([["วัน/เดือน/ปี (พ.ศ.)", "ผู้ทำรายการ", "การกระทำ", "รายละเอียด"]])
        .setFontWeight('bold')
        .setBackground('#fee2e2');
    }
    var timeInfo = getThaiDate(new Date());
    var actorName = String(actorOverride || getCurrentActorName_() || "System Admin").trim() || "System Admin";
    appendRowsSafely_(logSheet, [[timeInfo.dateTime, actorName, action, details]], 'บันทึก SystemLogs');
  } catch (e) {
    console.error("logSystemActionFast failed:", e);
  }
}


var OPERATING_DAY_CALENDAR_SETTING_KEY_ = 'OperatingDayCalendarJson';

function normalizeOperatingDayCalendar_(rawValue) {
  var source = rawValue;
  if (source === null || source === undefined || source === '') return {};

  if (typeof source === 'string') {
    var text = String(source || '').trim();
    if (!text) return {};
    try {
      source = JSON.parse(text);
    } catch (e) {
      return {};
    }
  }

  if (Object.prototype.toString.call(source) !== '[object Object]') return {};

  var normalized = {};
  Object.keys(source).sort().forEach(function(monthKey) {
    var normalizedMonthKey = String(monthKey || '').trim();
    var monthMatch = normalizedMonthKey.match(/^(\d{4})-(\d{2})$/);
    if (!monthMatch) return;

    var yearBe = Number(monthMatch[1]) || 0;
    var month = Number(monthMatch[2]) || 0;
    if (yearBe < 2400 || month < 1 || month > 12) return;

    var parts = parseReportDateInput_(source[monthKey]);
    if (!parts) return;
    if (Number(parts.yearBe) !== yearBe || Number(parts.month) !== month) return;

    var yearAd = Number(parts.yearBe) - 543;
    if (yearAd < 1900) return;
    normalized[normalizedMonthKey] = yearAd + '-' + padNumber_(parts.month, 2) + '-' + padNumber_(parts.day, 2);
  });

  return normalized;
}

function getOperatingDayCalendar_(settingsSheet) {
  if (!settingsSheet) return {};
  return normalizeOperatingDayCalendar_(getSettingValue_(settingsSheet, OPERATING_DAY_CALENDAR_SETTING_KEY_));
}
function getOperatingDayIsoByMonthKey_(settingsSheet, monthKey) {
  var normalizedMonthKey = String(monthKey || '').trim();
  if (!normalizedMonthKey) return '';
  var calendar = getOperatingDayCalendar_(settingsSheet);
  return String((calendar && calendar[normalizedMonthKey]) || '').trim();
}

function buildNoonDateFromIsoValue_(isoDate) {
  var normalizedIso = String(isoDate || '').trim();
  if (!/^\d{4}-\d{2}-\d{2}$/.test(normalizedIso)) return null;

  // ใช้เวลาเริ่มต้นวันแทน เพื่อไม่ให้เช้าวันทำการถูกตีความว่ายังไม่ถึงวันทำการ
  var nativeDate = new Date(normalizedIso + 'T00:00:00');
  return isNaN(nativeDate.getTime()) ? null : nativeDate;
}

function hasReachedOperatingDayForDate_(settingsSheet, referenceDate) {
  var compareDate = referenceDate instanceof Date ? new Date(referenceDate.getTime()) : new Date(referenceDate || new Date());
  if (isNaN(compareDate.getTime())) compareDate = new Date();
  var compareParts = parseThaiDateParts_(compareDate) || { yearBe: compareDate.getFullYear() + 543, month: compareDate.getMonth() + 1 };
  var monthKey = String(compareParts.yearBe || '').trim() + '-' + padNumber_(Number(compareParts.month) || 0, 2);
  var operatingIso = getOperatingDayIsoByMonthKey_(settingsSheet, monthKey);
  if (!operatingIso) return true;
  var operatingDate = buildNoonDateFromIsoValue_(operatingIso);
  if (!operatingDate) return true;
  return compareDate.getTime() >= operatingDate.getTime();
}

function getCloseAccountingReferenceContext_(settingsSheet, referenceDate) {
  var now = referenceDate instanceof Date ? new Date(referenceDate.getTime()) : new Date(referenceDate || new Date());
  if (isNaN(now.getTime())) now = new Date();
  var nowParts = parseThaiDateParts_(now) || { yearBe: now.getFullYear() + 543, month: now.getMonth() + 1 };
  var monthKey = String(nowParts.yearBe || (now.getFullYear() + 543)) + '-' + padNumber_(Number(nowParts.month) || (now.getMonth() + 1), 2);
  var operatingIso = getOperatingDayIsoByMonthKey_(settingsSheet, monthKey);
  var operatingDate = buildNoonDateFromIsoValue_(operatingIso);
  return {
    monthKey: monthKey,
    operatingDateIso: operatingIso,
    operatingDate: operatingDate,
    hasOperatingDay: !!operatingIso,
    hasReachedOperatingDay: operatingDate ? now.getTime() >= operatingDate.getTime() : true,
    now: now
  };
}

var REPORT_LAYOUT_SETTINGS_KEY_ = 'ReportLayoutSettingsJson';
var REPORT_LAYOUT_DEFAULTS_ = {
  pageMarginsCm: { top: 0.3, right: 0.18, bottom: 0.3, left: 0.18 },
  columnWidths: {
    daily: { no: 6, memberId: 8, name: 34, broughtForward: 14, principalPaid: 10, interestPaid: 10, balance: 10, note: 8 },
    register: { no: 8, date: 18, principalPaid: 16, interestPaid: 16, balance: 18, note: 24 },
    overdue_interest: { no: 6, memberId: 8, name: 36, broughtForward: 14, principalPaid: 9, interestPaid: 9, balance: 10, note: 8 }
  }
};

function cloneReportLayoutDefaults_() {
  return JSON.parse(JSON.stringify(REPORT_LAYOUT_DEFAULTS_));
}

function normalizeReportLayoutNumber_(value, fallback, minValue, maxValue) {
  var numeric = Number(value);
  if (!isFinite(numeric)) numeric = Number(fallback);
  if (!isFinite(numeric)) numeric = 0;
  if (isFinite(minValue)) numeric = Math.max(Number(minValue), numeric);
  if (isFinite(maxValue)) numeric = Math.min(Number(maxValue), numeric);
  return Math.round(numeric * 100) / 100;
}

function normalizeReportLayoutWidthMap_(source, defaultMap) {
  var safeSource = (source && Object.prototype.toString.call(source) === '[object Object]') ? source : {};
  var normalized = {};
  Object.keys(defaultMap || {}).forEach(function(key) {
    normalized[key] = normalizeReportLayoutNumber_(safeSource[key], defaultMap[key], 1, 100);
  });
  return normalized;
}

function normalizeReportLayoutSettings_(rawValue) {
  var defaults = cloneReportLayoutDefaults_();
  var source = rawValue;

  if (source === null || source === undefined || source === '') {
    return defaults;
  }

  if (typeof source === 'string') {
    var textValue = String(source || '').trim();
    if (!textValue) return defaults;
    try {
      source = JSON.parse(textValue);
    } catch (e) {
      return defaults;
    }
  }

  if (Object.prototype.toString.call(source) !== '[object Object]') return defaults;

  var pageMarginsSource = source.pageMarginsCm && Object.prototype.toString.call(source.pageMarginsCm) === '[object Object]' ? source.pageMarginsCm : {};
  var columnWidthsSource = source.columnWidths && Object.prototype.toString.call(source.columnWidths) === '[object Object]' ? source.columnWidths : {};

  return {
    pageMarginsCm: {
      top: normalizeReportLayoutNumber_(pageMarginsSource.top, defaults.pageMarginsCm.top, 0, 3),
      right: normalizeReportLayoutNumber_(pageMarginsSource.right, defaults.pageMarginsCm.right, 0, 3),
      bottom: normalizeReportLayoutNumber_(pageMarginsSource.bottom, defaults.pageMarginsCm.bottom, 0, 3),
      left: normalizeReportLayoutNumber_(pageMarginsSource.left, defaults.pageMarginsCm.left, 0, 3)
    },
    columnWidths: {
      daily: normalizeReportLayoutWidthMap_(columnWidthsSource.daily, defaults.columnWidths.daily),
      register: normalizeReportLayoutWidthMap_(columnWidthsSource.register, defaults.columnWidths.register),
      overdue_interest: normalizeReportLayoutWidthMap_(columnWidthsSource.overdue_interest, defaults.columnWidths.overdue_interest)
    }
  };
}

function getReportLayoutSettings_(settingsSheet) {
  if (!settingsSheet) return cloneReportLayoutDefaults_();
  return normalizeReportLayoutSettings_(getSettingValue_(settingsSheet, REPORT_LAYOUT_SETTINGS_KEY_));
}

function getOperatingDayCalendar() {
  requirePermission_('settings.view', 'ไม่มีสิทธิ์ดูวันทำการ');
  try {
    var ss = getDatabaseSpreadsheet_();
    var settingsSheet = ss.getSheetByName('Settings') || createSheetSafely_(ss, 'Settings', 3, 2, 'เตรียมชีต Settings');
    return { status: 'Success', calendar: getOperatingDayCalendar_(settingsSheet) };
  } catch (e) {
    return { status: 'Error', message: 'โหลดวันทำการไม่สำเร็จ: ' + e.toString(), calendar: {} };
  }
}

function saveOperatingDay(monthKey, isoDate) {
  requirePermission_('settings.manage', 'ไม่มีสิทธิ์แก้ไขวันทำการ');
  try {
    var normalizedMonthKey = String(monthKey || '').trim();
    var monthMatch = normalizedMonthKey.match(/^(\d{4})-(\d{2})$/);
    if (!monthMatch) {
      return { status: 'Error', message: 'รูปแบบเดือนของวันทำการไม่ถูกต้อง', calendar: {} };
    }

    var yearBe = Number(monthMatch[1]) || 0;
    var month = Number(monthMatch[2]) || 0;
    if (yearBe < 2400 || month < 1 || month > 12) {
      return { status: 'Error', message: 'เดือนของวันทำการไม่ถูกต้อง', calendar: {} };
    }

    var normalizedIsoDate = String(isoDate || '').trim();
    var ss = getDatabaseSpreadsheet_();
    var settingsSheet = ss.getSheetByName('Settings') || createSheetSafely_(ss, 'Settings', 3, 2, 'เตรียมชีต Settings');
    var calendar = getOperatingDayCalendar_(settingsSheet);

    if (normalizedIsoDate) {
      var parts = parseReportDateInput_(normalizedIsoDate);
      if (!parts) {
        return { status: 'Error', message: 'รูปแบบวันที่วันทำการไม่ถูกต้อง', calendar: calendar };
      }
      if (Number(parts.yearBe) !== yearBe || Number(parts.month) !== month) {
        return { status: 'Error', message: 'วันที่วันทำการต้องอยู่ในเดือนเดียวกับงวดที่กำหนด', calendar: calendar };
      }
      normalizedIsoDate = (Number(parts.yearBe) - 543) + '-' + padNumber_(parts.month, 2) + '-' + padNumber_(parts.day, 2);
      calendar[normalizedMonthKey] = normalizedIsoDate;
    } else {
      delete calendar[normalizedMonthKey];
    }

    upsertSettingValue_(settingsSheet, OPERATING_DAY_CALENDAR_SETTING_KEY_, JSON.stringify(calendar));
    clearAppCache_();
    logSystemActionFast(ss, 'บันทึกวันทำการ', normalizedIsoDate ? ('กำหนดวันทำการ ' + normalizedMonthKey + ' = ' + normalizedIsoDate) : ('ลบวันทำการ ' + normalizedMonthKey));

    return {
      status: 'Success',
      monthKey: normalizedMonthKey,
      operatingDate: normalizedIsoDate,
      calendar: calendar,
      message: normalizedIsoDate ? 'บันทึกวันทำการเรียบร้อยแล้ว' : 'ลบวันทำการเรียบร้อยแล้ว'
    };
  } catch (e) {
    return { status: 'Error', message: 'บันทึกวันทำการไม่สำเร็จ: ' + e.toString(), calendar: {} };
  }
}

function saveSettings(settingsStr) {
  requireAdminSession_();
  try {
    var ss = getDatabaseSpreadsheet_();
    var settingsData = JSON.parse(settingsStr);

    var settingsSheet = ss.getSheetByName("Settings") || createSheetSafely_(ss, "Settings", 3, 2, 'เตรียมชีต Settings');

    if (settingsSheet.getLastRow() === 0) {
      ensureSheetGridSizeSafely_(settingsSheet, 1, 2, 'เตรียมชีต Settings');
      settingsSheet.getRange(1, 1, 1, 2).setValues([["หัวข้อการตั้งค่า", "ค่าการตั้งค่า"]]).setFontWeight("bold");
    }

    var sData = settingsSheet.getRange(1, 1, settingsSheet.getLastRow(), 2).getValues();
    var foundInterest = false;

    for (var i = 0; i < sData.length; i++) {
      if (sData[i][0] === "InterestRate") {
        settingsSheet.getRange(i + 1, 2).setValue(Number(settingsData.interestRate) || 12.0);
        foundInterest = true;
        break;
      }
    }

    if (!foundInterest) {
      appendRowsSafely_(settingsSheet, [["InterestRate", Number(settingsData.interestRate) || 12.0]], 'เพิ่มค่า InterestRate ใน Settings');
    }

    // ตั้งค่าอัตราดอกเบี้ยเป็นค่าเริ่มต้นสำหรับสัญญาใหม่เท่านั้น
    // ไม่อัปเดตสัญญาเก่าย้อนหลัง เพื่อป้องกันยอดดอกเบี้ยเพี้ยนทั้งพอร์ต

    clearAppCache_();
    return "Success";
  } catch (e) {
    return "Error: " + e.toString();
  }
}

// ==========================================
// 6. INITIAL SETUP
// ==========================================

function setPlainTextColumnFormat_(sheet, columnIndexes) {
  if (!sheet || !columnIndexes || !columnIndexes.length) return;

  var spreadsheetId = '';
  var sheetId = '';
  try {
    spreadsheetId = String(sheet.getParent().getId());
    sheetId = String(sheet.getSheetId());
  } catch (e) {
    spreadsheetId = 'unknown';
    sheetId = String(sheet.getName() || 'sheet');
  }

  var props = PropertiesService.getScriptProperties();
  var targetRows = Math.max(200, (sheet.getLastRow() || 0) + 200);

  for (var i = 0; i < columnIndexes.length; i++) {
    var columnIndex = Number(columnIndexes[i]) || 0;
    if (columnIndex <= 0) continue;

    var watermarkKey = 'PLAIN_TEXT_FMT::' + spreadsheetId + '::' + sheetId + '::' + columnIndex;
    var formattedRows = Number(props.getProperty(watermarkKey) || 0) || 0;
    if (formattedRows >= targetRows) continue;

    var startRow = Math.max(1, formattedRows + 1);
    var rowCount = Math.max(0, targetRows - formattedRows);
    if (rowCount <= 0) continue;

    sheet.getRange(startRow, columnIndex, rowCount, 1).setNumberFormat('@');
    props.setProperty(watermarkKey, String(targetRows));
  }
}

function extractTimeTextFromValue_(value) {
  if (value === null || value === undefined || value === '') return '00:00:00';

  var cacheKey = getExecutionValueCacheKey_(value);
  if (EXECUTION_TIME_TEXT_CACHE_.hasOwnProperty(cacheKey)) {
    return EXECUTION_TIME_TEXT_CACHE_[cacheKey];
  }

  var result = '00:00:00';
  var normalizedDate = normalizeDateObjectForThai_(value);
  if (normalizedDate) {
    result = padNumber_(normalizedDate.getHours(), 2) + ':' + padNumber_(normalizedDate.getMinutes(), 2) + ':' + padNumber_(normalizedDate.getSeconds(), 2);
    EXECUTION_TIME_TEXT_CACHE_[cacheKey] = result;
    return result;
  }

  var text = String(value || '').trim();
  var timeMatch = text.match(/(\d{1,2}):(\d{2})(?::(\d{2}))?/);
  if (timeMatch) {
    result = padNumber_(Number(timeMatch[1]) || 0, 2) + ':' + padNumber_(Number(timeMatch[2]) || 0, 2) + ':' + padNumber_(Number(timeMatch[3]) || 0, 2);
  }

  EXECUTION_TIME_TEXT_CACHE_[cacheKey] = result;
  return result;
}

function normalizeThaiDateCellValue_(rawValue, includeTime) {
  if (rawValue === null || rawValue === undefined || rawValue === '') return '';

  var cacheKey = getExecutionValueCacheKey_(rawValue) + '::' + (includeTime ? '1' : '0');
  if (EXECUTION_NORMALIZED_THAI_DATE_CACHE_.hasOwnProperty(cacheKey)) {
    return EXECUTION_NORMALIZED_THAI_DATE_CACHE_[cacheKey];
  }

  var parsed = parseThaiDateParts_(rawValue);
  var result = '';
  if (!parsed || !parsed.dateOnly) {
    result = String(rawValue || '').trim();
  } else if (!includeTime) {
    result = parsed.dateOnly;
  } else {
    result = parsed.dateOnly + ' ' + extractTimeTextFromValue_(rawValue);
  }

  EXECUTION_NORMALIZED_THAI_DATE_CACHE_[cacheKey] = result;
  return result;
}

function normalizeDateColumnValues_(sheet, columnIndex, includeTime) {
  if (!sheet || sheet.getLastRow() <= 1) return;

  var range = sheet.getRange(2, columnIndex, sheet.getLastRow() - 1, 1);
  var values = range.getValues();
  var changed = false;

  for (var i = 0; i < values.length; i++) {
    var rawValue = values[i][0];
    if (rawValue === null || rawValue === undefined || rawValue === '') continue;

    var normalized = normalizeThaiDateCellValue_(rawValue, includeTime);
    if (normalized && String(rawValue) !== normalized) {
      values[i][0] = normalized;
      changed = true;
    }
  }

  range.setNumberFormat('@');
  if (changed) range.setValues(values);
}

function runBuddhistDateMigrationIfNeeded_(ss) {
  if (!ss) return;

  var props = PropertiesService.getDocumentProperties();
  var migrationKey = 'BUDDHIST_DATE_MIGRATION_V2';
  if (props.getProperty(migrationKey) === 'done') return;

  var loanSheet = ss.getSheetByName('Loans');
  if (loanSheet) normalizeDateColumnValues_(loanSheet, 10, false);

  var currentTransactionSheets = listCurrentTransactionSheets_(ss);
  for (var t = 0; t < currentTransactionSheets.length; t++) {
    normalizeDateColumnValues_(currentTransactionSheets[t], 2, true);
  }

  var logsSheet = ss.getSheetByName('SystemLogs');
  if (logsSheet) normalizeDateColumnValues_(logsSheet, 1, true);

  var archiveSheets = listAllTransactionArchiveSheets_(ss);
  for (var i = 0; i < archiveSheets.length; i++) {
    var sheet = archiveSheets[i];
    normalizeDateColumnValues_(sheet, 2, true);
    if (sheet.getLastColumn() >= 15) normalizeDateColumnValues_(sheet, 15, true);
  }

  props.setProperty(migrationKey, 'done');
}


function ensureRuntimeDatabase_() {
  var ss = getDatabaseSpreadsheet_();

  if (
    isRuntimeDatabaseWarm_(ss) &&
    ss.getSheetByName('Loans') &&
    ss.getSheetByName('Members') &&
    ss.getSheetByName('Transactions') &&
    ss.getSheetByName('Settings')
  ) {
    return ss;
  }

  var ensureSheetIfMissing = function(sheetName, headers, bgColor) {
    return ensureSheetWithHeadersSafely_(ss, sheetName, headers, bgColor, 'เตรียมชีต ' + sheetName);
  };

  var loansSheet = ensureSheetIfMissing('Loans', [
    'รหัสสมาชิก', 'เลขที่สัญญา', 'ชื่อ-สกุลผู้กู้', 'ยอดเงินกู้', 'ดอกเบี้ย (%)',
    'ยอดคงค้าง', 'สถานะ', 'วันครบกำหนด', 'จำนวนเดือนค้างชำระ',
    'วันที่สร้างสัญญา', 'ผู้ค้ำประกัน 1', 'ผู้ค้ำประกัน 2'
  ], '#d1fae5');
  setPlainTextColumnFormat_(loansSheet, [1, 2, 8, 10, 11, 12]);

  var membersSheet = ensureSheetIfMissing('Members', [
    'รหัสสมาชิก', 'ชื่อ-สกุล', 'สถานะ'
  ], '#e0e7ff');
  setPlainTextColumnFormat_(membersSheet, [1]);

  var usersSheet = ensureUsersSheet_(ss);
  setPlainTextColumnFormat_(usersSheet, [1, 2, 4, 5, 8, 9, 10, 11, 12, 13, 14]);

  var transactionsSheet = ensureSheetIfMissing('Transactions', [
    'รหัสอ้างอิง', 'วัน/เดือน/ปี (พ.ศ.)', 'เลขที่สัญญา', 'รหัสสมาชิก',
    'ชำระเงินต้น', 'ชำระดอกเบี้ย', 'ยอดคงเหลือ', 'หมายเหตุ',
    'ผู้ทำรายการ', 'ชื่อ-สกุล', 'สถานะการทำรายการ',
    'จำนวนงวดดอกที่ชำระ', 'ค้างดอกก่อนรับชำระ', 'ค้างดอกหลังรับชำระ'
  ], '#fffbeb');
  ensureTransactionsSheetSchema_(transactionsSheet);
  setPlainTextColumnFormat_(transactionsSheet, [1, 2, 3, 4]);

  var systemLogsSheet = ensureSheetIfMissing('SystemLogs', [
    'วัน/เดือน/ปี (พ.ศ.)', 'ผู้ทำรายการ', 'การกระทำ', 'รายละเอียด'
  ], '#fee2e2');
  setPlainTextColumnFormat_(systemLogsSheet, [1]);

  ensureAuditLogsSheet_(ss);
  ensureCountersSheet_(ss);

  var settingsSheet = ss.getSheetByName('Settings') || createSheetSafely_(ss, 'Settings', 3, 2, 'เตรียมชีต Settings');
  if (settingsSheet.getLastRow() === 0) {
    settingsSheet.getRange(1, 1, 3, 2).setValues([
      ['หัวข้อการตั้งค่า', 'ค่าการตั้งค่า'],
      ['InterestRate', 12.0],
      ['AdminPasswordHash', createPasswordHashRecord_('admin')]
    ]).setFontWeight('bold');
  }

  markRuntimeDatabaseWarm_(ss);
  return ss;
}

function setupDatabase() {
  requirePermission_('settings.manage', 'ไม่มีสิทธิ์ตั้งค่าฐานข้อมูลอัตโนมัติ');
  var ss = getDatabaseSpreadsheet_();

  // ป้องกันการลบข้อมูลโดยไม่ตั้งใจ ถ้าชีตมีข้อมูลอยู่แล้วจะไม่ล้างชีต
  var createSheetIfNotExistsAndEmpty = function(sheetName, headers, bgColor) {
    return ensureSheetWithHeadersSafely_(ss, sheetName, headers, bgColor, 'Automated Setup / ' + sheetName);
  };

  var loansSheet = createSheetIfNotExistsAndEmpty("Loans", [
    "รหัสสมาชิก", "เลขที่สัญญา", "ชื่อ-สกุลผู้กู้", "ยอดเงินกู้", "ดอกเบี้ย (%)",
    "ยอดคงค้าง", "สถานะ", "วันครบกำหนด", "จำนวนเดือนค้างชำระ",
    "วันที่สร้างสัญญา", "ผู้ค้ำประกัน 1", "ผู้ค้ำประกัน 2"
  ], "#d1fae5");
  setPlainTextColumnFormat_(loansSheet, [1, 2, 8, 10, 11, 12]);

  var membersSheet = createSheetIfNotExistsAndEmpty("Members", [
    "รหัสสมาชิก", "ชื่อ-สกุล", "สถานะ"
  ], "#e0e7ff");

  var usersSheet = ensureUsersSheet_(ss);
  setPlainTextColumnFormat_(usersSheet, [1, 2, 4, 5, 8, 9, 10, 11, 12, 13, 14]);

  if (membersSheet.getLastRow() > 0 && membersSheet.getLastColumn() > 0) {
    var memberHeaders = membersSheet.getRange(1, 1, 1, membersSheet.getLastColumn()).getValues()[0];
    var legacySavingsCol = memberHeaders.indexOf("เงินฝากสัจจะประเมิน");
    if (legacySavingsCol !== -1) {
      membersSheet.deleteColumn(legacySavingsCol + 1);
    }
  }

  var transactionsSheet = createSheetIfNotExistsAndEmpty("Transactions", [
    "รหัสอ้างอิง", "วัน/เดือน/ปี (พ.ศ.)", "เลขที่สัญญา", "รหัสสมาชิก",
    "ชำระเงินต้น", "ชำระดอกเบี้ย", "ยอดคงเหลือ", "หมายเหตุ",
    "ผู้ทำรายการ", "ชื่อ-สกุล", "สถานะการทำรายการ",
    "จำนวนงวดดอกที่ชำระ", "ค้างดอกก่อนรับชำระ", "ค้างดอกหลังรับชำระ"
  ], "#fffbeb");
  ensureTransactionsSheetSchema_(transactionsSheet);
  setPlainTextColumnFormat_(transactionsSheet, [1, 2, 3, 4]);

  var systemLogsSheet = createSheetIfNotExistsAndEmpty("SystemLogs", [
    "วัน/เดือน/ปี (พ.ศ.)", "ผู้ทำรายการ", "การกระทำ", "รายละเอียด"
  ], "#fee2e2");
  setPlainTextColumnFormat_(systemLogsSheet, [1]);

  ensureAuditLogsSheet_(ss);
  ensureCountersSheet_(ss);

  var settingsSheet = ss.getSheetByName("Settings") || createSheetSafely_(ss, "Settings", 3, 2, 'Automated Setup / Settings');
  if (settingsSheet.getLastRow() === 0) {
    settingsSheet.getRange(1, 1, 3, 2).setValues([
      ["หัวข้อการตั้งค่า", "ค่าการตั้งค่า"],
      ["InterestRate", 12.0],
      ["AdminPasswordHash", createPasswordHashRecord_("admin")] // รหัสผ่านเริ่มต้นคือ admin
    ]).setFontWeight("bold");
  }

  runBuddhistDateMigrationIfNeeded_(ss);

  appendAuditLogSafe_(ss, {
    action: 'SETUP_DATABASE',
    entityType: 'SYSTEM',
    entityId: 'Spreadsheet',
    details: 'ตั้งค่าโครงสร้างฐานข้อมูลอัตโนมัติสำเร็จ'
  });

  clearAppCache_();
  return "Success";
}


// ==========================================
// 8. END OF DAY CLOSE
// ==========================================

function buildThaiDateParts_(day, month, yearBe, timeParts) {
  day = Number(day) || 0;
  month = Number(month) || 0;
  yearBe = Number(yearBe) || 0;
  var hour = Number(timeParts && timeParts.hour) || 0;
  var minute = Number(timeParts && timeParts.minute) || 0;
  var second = Number(timeParts && timeParts.second) || 0;

  if (yearBe > 0 && yearBe < 2400) yearBe += 543;
  if (!day || !month || !yearBe) return null;

  var gregYear = yearBe - 543;
  var nativeDate = new Date(gregYear, month - 1, day, hour, minute, second, 0);
  if (isNaN(nativeDate.getTime()) ||
      nativeDate.getFullYear() !== gregYear ||
      nativeDate.getMonth() !== month - 1 ||
      nativeDate.getDate() !== day) {
    return null;
  }

  return {
    day: day,
    month: month,
    yearBe: yearBe,
    hour: hour,
    minute: minute,
    second: second,
    nativeDate: nativeDate,
    dateOnly: padNumber_(day, 2) + '/' + padNumber_(month, 2) + '/' + yearBe,
    dateTime: padNumber_(day, 2) + '/' + padNumber_(month, 2) + '/' + yearBe + ' ' + padNumber_(hour, 2) + ':' + padNumber_(minute, 2) + ':' + padNumber_(second, 2)
  };
}

function parseThaiDateParts_(value) {
  if (value === null || value === undefined || value === '') return null;

  var cacheKey = getExecutionValueCacheKey_(value);
  if (EXECUTION_THAI_DATE_PARTS_CACHE_.hasOwnProperty(cacheKey)) {
    return EXECUTION_THAI_DATE_PARTS_CACHE_[cacheKey];
  }

  var result = null;

  if (Object.prototype.toString.call(value) === '[object Date]') {
    var normalizedDate = normalizeDateObjectForThai_(value);
    result = normalizedDate
      ? buildThaiDateParts_(
          normalizedDate.getDate(),
          normalizedDate.getMonth() + 1,
          normalizedDate.getFullYear() + 543,
          {
            hour: normalizedDate.getHours(),
            minute: normalizedDate.getMinutes(),
            second: normalizedDate.getSeconds()
          }
        )
      : null;
    EXECUTION_THAI_DATE_PARTS_CACHE_[cacheKey] = result;
    return result;
  }

  if (typeof value === 'number' && !isNaN(value)) {
    result = parseThaiDateParts_(new Date(value));
    EXECUTION_THAI_DATE_PARTS_CACHE_[cacheKey] = result;
    return result;
  }

  var text = String(value || '').trim();
  if (!text) {
    EXECUTION_THAI_DATE_PARTS_CACHE_[cacheKey] = null;
    return null;
  }

  var timeMatch = text.match(/(\d{1,2}):(\d{1,2})(?::(\d{1,2}))?/);
  var parsedTime = {
    hour: Number(timeMatch && timeMatch[1]) || 0,
    minute: Number(timeMatch && timeMatch[2]) || 0,
    second: Number(timeMatch && timeMatch[3]) || 0
  };

  var datePart = text.split(' ')[0].trim();
  var slashMatch = datePart.match(/^(\d{1,2})\/(\d{1,2})\/(\d{4})$/);
  if (slashMatch) {
    result = buildThaiDateParts_(slashMatch[1], slashMatch[2], slashMatch[3], parsedTime);
    EXECUTION_THAI_DATE_PARTS_CACHE_[cacheKey] = result;
    return result;
  }

  var isoMatch = text.match(/^(\d{4})-(\d{1,2})-(\d{1,2})/);
  if (isoMatch) {
    result = buildThaiDateParts_(isoMatch[3], isoMatch[2], Number(isoMatch[1]) + 543, parsedTime);
    EXECUTION_THAI_DATE_PARTS_CACHE_[cacheKey] = result;
    return result;
  }

  var yearMatch = text.match(/\b(2\d{3})\b/);
  var cleanedNativeText = text.replace(/\s*\([^)]*\)\s*$/, '');
  var nativeDate = new Date(cleanedNativeText);
  if (!isNaN(nativeDate.getTime())) {
    if (yearMatch) {
      var rawYear = Number(yearMatch[1]) || 0;
      if (rawYear >= 2400 && rawYear <= 2600) {
        nativeDate.setFullYear(nativeDate.getFullYear() - 543);
      }
    }
    result = parseThaiDateParts_(nativeDate);
    EXECUTION_THAI_DATE_PARTS_CACHE_[cacheKey] = result;
    return result;
  }

  EXECUTION_THAI_DATE_PARTS_CACHE_[cacheKey] = null;
  return null;
}


/***** CELL LIMIT GUARD *****/
var SPREADSHEET_MAX_CELLS_ = 10000000;
var SPREADSHEET_CELL_SAFE_BUFFER_ = 50000;
var NEW_SHEET_DEFAULT_ROWS_ = 1000;
var NEW_SHEET_DEFAULT_COLS_ = 26;

function getSpreadsheetAllocatedCellCount_(ss) {
  var sheets = ss.getSheets();
  var total = 0;
  for (var i = 0; i < sheets.length; i++) {
    total += (sheets[i].getMaxRows() * sheets[i].getMaxColumns());
  }
  return total;
}

function estimateSheetGrowthCells_(sheet, targetRows, targetCols) {
  var currentRows = sheet.getMaxRows();
  var currentCols = sheet.getMaxColumns();
  var nextRows = Math.max(currentRows, targetRows);
  var nextCols = Math.max(currentCols, targetCols);
  return Math.max(0, (nextRows * nextCols) - (currentRows * currentCols));
}

function throwCellLimitExceededError_(actionLabel, usedCells, requestedCells) {
  var safeLimit = SPREADSHEET_MAX_CELLS_ - SPREADSHEET_CELL_SAFE_BUFFER_;
  var remaining = Math.max(0, safeLimit - usedCells);
  throw new Error(
    'CELL_LIMIT_EXCEEDED: ไม่สามารถ' + String(actionLabel || 'ขยายชีต') +
    ' ได้ เพราะไฟล์ใกล้/เกินเพดาน 10,000,000 เซลล์' +
    ' | ใช้แล้ว ' + usedCells.toLocaleString('en-US') +
    ' เซลล์ | ต้องเพิ่มอีก ' + requestedCells.toLocaleString('en-US') +
    ' เซลล์ | เหลือปลอดภัยประมาณ ' + remaining.toLocaleString('en-US') + ' เซลล์'
  );
}

function assertSpreadsheetCanGrow_(ss, additionalCells, actionLabel) {
  additionalCells = Math.max(0, Number(additionalCells) || 0);
  if (additionalCells <= 0) return;

  var usedCells = getSpreadsheetAllocatedCellCount_(ss);
  var safeLimit = SPREADSHEET_MAX_CELLS_ - SPREADSHEET_CELL_SAFE_BUFFER_;

  if ((usedCells + additionalCells) > safeLimit) {
    throwCellLimitExceededError_(actionLabel, usedCells, additionalCells);
  }
}

function ensureSheetGridSizeSafely_(sheet, targetRows, targetCols, actionLabel) {
  if (!sheet) throw new Error('ไม่พบชีตสำหรับขยายขนาด');

  targetRows = Math.max(1, Number(targetRows) || sheet.getMaxRows() || 1);
  targetCols = Math.max(1, Number(targetCols) || sheet.getMaxColumns() || 1);

  var growthCells = estimateSheetGrowthCells_(sheet, targetRows, targetCols);
  if (growthCells <= 0) return;

  assertSpreadsheetCanGrow_(
    sheet.getParent(),
    growthCells,
    actionLabel || ('ขยายชีต ' + sheet.getName())
  );

  var addCols = targetCols - sheet.getMaxColumns();
  if (addCols > 0) {
    sheet.insertColumnsAfter(sheet.getMaxColumns(), addCols);
  }

  var addRows = targetRows - sheet.getMaxRows();
  if (addRows > 0) {
    sheet.insertRowsAfter(sheet.getMaxRows(), addRows);
  }

  invalidateSheetExecutionCaches_(sheet);
  invalidateSpreadsheetExecutionCaches_(sheet.getParent());
}

function createSheetSafely_(ss, sheetName, minRows, minCols, actionLabel) {
  var existing = ss.getSheetByName(sheetName);
  if (existing) return existing;

  assertSpreadsheetCanGrow_(
    ss,
    NEW_SHEET_DEFAULT_ROWS_ * NEW_SHEET_DEFAULT_COLS_,
    actionLabel || ('สร้างชีต ' + sheetName)
  );

  var sheet = ss.insertSheet(sheetName);
  invalidateSpreadsheetExecutionCaches_(ss);

  var requiredRows = Math.max(NEW_SHEET_DEFAULT_ROWS_, Number(minRows) || 1);
  var requiredCols = Math.max(NEW_SHEET_DEFAULT_COLS_, Number(minCols) || 1);
  ensureSheetGridSizeSafely_(sheet, requiredRows, requiredCols, actionLabel || ('สร้างชีต ' + sheetName));
  return sheet;
}

function ensureSheetWithHeadersSafely_(ss, sheetName, headers, bgColor, actionLabel) {
  var headerCount = Math.max(1, Array.isArray(headers) ? headers.length : 1);
  var sheet = ss.getSheetByName(sheetName) || createSheetSafely_(ss, sheetName, 2, headerCount, actionLabel || ('เตรียมชีต ' + sheetName));

  ensureSheetGridSizeSafely_(
    sheet,
    Math.max(sheet.getMaxRows(), 2),
    Math.max(sheet.getMaxColumns(), headerCount),
    actionLabel || ('เตรียมชีต ' + sheetName)
  );

  var shouldWriteHeaders = sheet.getLastRow() === 0;
  if (!shouldWriteHeaders) {
    var currentHeaders = sheet.getRange(1, 1, 1, headerCount).getValues()[0];
    for (var i = 0; i < headerCount; i++) {
      if (String(currentHeaders[i] || '').trim() !== String(headers[i] || '').trim()) {
        shouldWriteHeaders = true;
        break;
      }
    }
  }

  if (shouldWriteHeaders) {
    var headerRange = sheet.getRange(1, 1, 1, headerCount);
    headerRange.setValues([headers]).setFontWeight('bold');
    if (bgColor) headerRange.setBackground(bgColor);
    invalidateSheetExecutionCaches_(sheet);
  }

  return sheet;
}

function ensureSheetCapacityForAppend_(sheet, rowsToAppend, requiredCols, actionLabel) {
  rowsToAppend = Math.max(0, Number(rowsToAppend) || 0);
  requiredCols = Math.max(1, Number(requiredCols) || 1);
  if (rowsToAppend <= 0) return;

  var targetRows = Math.max(sheet.getMaxRows(), sheet.getLastRow() + rowsToAppend);
  var targetCols = Math.max(sheet.getMaxColumns(), requiredCols);
  ensureSheetGridSizeSafely_(sheet, targetRows, targetCols, actionLabel || ('เพิ่มข้อมูลในชีต ' + sheet.getName()));
}

function appendRowsSafely_(sheet, rows, actionLabel) {
  if (!sheet || !Array.isArray(rows) || rows.length === 0) {
    return sheet ? (sheet.getLastRow() + 1) : 1;
  }

  var normalized = [];
  var colCount = 0;

  for (var i = 0; i < rows.length; i++) {
    var row = Array.isArray(rows[i]) ? rows[i].slice() : [];
    colCount = Math.max(colCount, row.length);
    normalized.push(row);
  }

  colCount = Math.max(1, colCount);

  for (var r = 0; r < normalized.length; r++) {
    while (normalized[r].length < colCount) normalized[r].push('');
  }

  ensureSheetCapacityForAppend_(sheet, normalized.length, colCount, actionLabel || ('เพิ่มข้อมูลในชีต ' + sheet.getName()));

  var startRow = sheet.getLastRow() + 1;
  sheet.getRange(startRow, 1, normalized.length, colCount).setValues(normalized);
  invalidateSheetExecutionCaches_(sheet);
  return startRow;
}

function assertSheetAppendPlanCapacity_(ss, sheetAppendCountMap, requiredCols, actionLabel) {
  var totalAdditionalCells = 0;
  var keys = Object.keys(sheetAppendCountMap || {});
  var normalizedCols = Math.max(1, Number(requiredCols) || 1);

  for (var i = 0; i < keys.length; i++) {
    var sheetName = keys[i];
    var rowsToAppend = Math.max(0, Number(sheetAppendCountMap[sheetName]) || 0);
    if (rowsToAppend <= 0) continue;

    var sheet = ss.getSheetByName(sheetName);
    if (!sheet) {
      var finalRows = Math.max(NEW_SHEET_DEFAULT_ROWS_, rowsToAppend + 1);
      var finalCols = Math.max(NEW_SHEET_DEFAULT_COLS_, normalizedCols);
      totalAdditionalCells += (finalRows * finalCols);
      continue;
    }

    var targetRows = Math.max(sheet.getMaxRows(), sheet.getLastRow() + rowsToAppend);
    var targetCols = Math.max(sheet.getMaxColumns(), normalizedCols);
    totalAdditionalCells += estimateSheetGrowthCells_(sheet, targetRows, targetCols);
  }

  assertSpreadsheetCanGrow_(ss, totalAdditionalCells, actionLabel || 'เพิ่มข้อมูลหลายชีต');
}

function getSettingValue_(settingsSheet, key) {
  if (!settingsSheet || settingsSheet.getLastRow() === 0) return '';
  var settingsMap = getSettingsMapCached_(settingsSheet);
  var normalizedKey = String(key || '').trim();
  return settingsMap.hasOwnProperty(normalizedKey) ? settingsMap[normalizedKey] : '';
}

function upsertSettingValue_(settingsSheet, key, value) {
  if (!settingsSheet) return;

  if (settingsSheet.getLastRow() === 0) {
    ensureSheetGridSizeSafely_(settingsSheet, 1, 2, 'เตรียมชีต Settings');
    settingsSheet.getRange(1, 1, 1, 2).setValues([['หัวข้อการตั้งค่า', 'ค่าการตั้งค่า']]).setFontWeight('bold');
  }

  var normalizedKey = String(key || '').trim();
  var data = settingsSheet.getRange(1, 1, settingsSheet.getLastRow(), 2).getValues();
  for (var i = 0; i < data.length; i++) {
    if (String(data[i][0] || '').trim() === normalizedKey) {
      settingsSheet.getRange(i + 1, 2).setValue(value);
      invalidateSpreadsheetExecutionCaches_(settingsSheet.getParent());
      return;
    }
  }

  appendRowsSafely_(settingsSheet, [[normalizedKey, value]], 'เพิ่มค่าตั้งค่าในชีต Settings');
  invalidateSpreadsheetExecutionCaches_(settingsSheet.getParent());
}

function getTransactionsArchiveSheetName_(buddhistYear, month) {
  var normalizedYearBe = Number(buddhistYear) || 0;
  var normalizedMonth = Number(month) || 0;
  if (!normalizedYearBe) return 'Transactions';
  if (normalizedMonth >= 1 && normalizedMonth <= 12) {
    return 'Transactions_' + normalizedYearBe + '_' + padNumber_(normalizedMonth, 2);
  }
  return 'Transactions_' + normalizedYearBe;
}

function parseTransactionsArchiveSheetMeta_(sheetName) {
  var normalizedName = String(sheetName || '').trim();
  if (!normalizedName || normalizedName === 'Transactions') return null;

  var match = normalizedName.match(/^Transactions_(\d{4})(?:_(\d{2}))?$/);
  if (!match) return null;

  var yearBe = Number(match[1]) || 0;
  var month = match[2] ? (Number(match[2]) || 0) : 0;
  if (!yearBe) return null;
  if (match[2] && (month < 1 || month > 12)) return null;

  return {
    sheetName: normalizedName,
    yearBe: yearBe,
    month: month,
    isMonthly: month >= 1 && month <= 12,
    isLegacyYearOnly: !match[2]
  };
}

function isTransactionSheetName_(sheetName) {
  return /^Transactions(?:_\d{4}(?:_\d{2})?)?$/.test(String(sheetName || ''));
}

function pushUniqueTransactionSheet_(collector, seenNames, sheet) {
  if (!sheet) return;
  var name = String(sheet.getName() || '');
  if (!name || seenNames[name]) return;
  collector.push(sheet);
  seenNames[name] = true;
}

function listCurrentTransactionSheets_(ss) {
  var sheets = [];
  var seenNames = {};
  if (!ss) return sheets;

  pushUniqueTransactionSheet_(sheets, seenNames, ss.getSheetByName('Transactions'));

  return sheets;
}

function resolveWritableTransactionsSheet_(ss) {
  if (!ss) throw new Error('ไม่พบ Spreadsheet สำหรับเขียนธุรกรรม');

  var currentSheets = listCurrentTransactionSheets_(ss);
  if (currentSheets.length > 0) {
    var preferred = ss.getSheetByName('Transactions');
    if (preferred) {
      ensureTransactionsSheetSchema_(preferred);
      return preferred;
    }

    ensureTransactionsSheetSchema_(currentSheets[0]);
    return currentSheets[0];
  }

  var createdSheet = createSheetSafely_(ss, 'Transactions', 2, 14, 'เตรียมชีตธุรกรรมปัจจุบัน');
  invalidateSpreadsheetExecutionCaches_(ss);
  ensureTransactionsSheetSchema_(createdSheet);
  return createdSheet;
}

function findTransactionInCurrentSheetsById_(ss, txId) {
  if (!ss || !txId) return null;

  var indexedMatch = getIndexedTransactionMatchById_(ss, txId);
  if (indexedMatch) {
    var currentSheetNames = getCurrentTransactionSheetNameSet_(ss);
    var indexedSheetName = String(indexedMatch.sheet && indexedMatch.sheet.getName ? indexedMatch.sheet.getName() || '' : '').trim();
    if (indexedSheetName && currentSheetNames[indexedSheetName]) {
      return indexedMatch;
    }
  }

  var currentSheets = listCurrentTransactionSheets_(ss);
  for (var i = 0; i < currentSheets.length; i++) {
    var matchedTx = findTransactionByIdInSheet_(currentSheets[i], txId);
    if (matchedTx) return matchedTx;
  }

  return null;
}


function findTransactionByIdInSheet_(sheet, txId) {
  if (!sheet || !txId || sheet.getLastRow() <= 1) return null;

  var finder = sheet.getRange(2, 1, sheet.getLastRow() - 1, 1).createTextFinder(String(txId)).matchEntireCell(true).findNext();
  if (!finder) return null;

  var rowIndex = finder.getRow();
  var readCols = Math.min(sheet.getLastColumn(), 16);
  return {
    sheet: sheet,
    rowIndex: rowIndex,
    values: sheet.getRange(rowIndex, 1, 1, readCols).getValues()[0]
  };
}


function findExistingTransactionById_(ss, txId) {
  if (!ss || !txId) return null;

  var indexedMatch = getIndexedTransactionMatchById_(ss, txId);
  if (indexedMatch) return indexedMatch;

  var sheetsToScan = listCurrentTransactionSheets_(ss).concat(listAllTransactionArchiveSheets_(ss));
  for (var i = 0; i < sheetsToScan.length; i++) {
    var matchedTx = findTransactionByIdInSheet_(sheetsToScan[i], txId);
    if (matchedTx) return matchedTx;
  }

  return null;
}


function listTransactionArchiveSheetsByYear_(ss, buddhistYear, month) {
  var sheets = [];
  var seenNames = {};
  var normalizedYearBe = Number(buddhistYear) || 0;
  var normalizedMonth = Number(month) || 0;
  if (!ss || !normalizedYearBe) return sheets;

  var archiveMetas = getTransactionArchiveSheetsMetaCached_(ss);
  for (var i = 0; i < archiveMetas.length; i++) {
    var entry = archiveMetas[i];
    var meta = entry.meta || {};
    if (Number(meta.yearBe) !== normalizedYearBe) continue;
    if (normalizedMonth && meta.isMonthly && Number(meta.month) !== normalizedMonth) continue;
    pushUniqueTransactionSheet_(sheets, seenNames, entry.sheet);
  }

  sheets.sort(function(a, b) {
    var metaA = parseTransactionsArchiveSheetMeta_(a.getName()) || {};
    var metaB = parseTransactionsArchiveSheetMeta_(b.getName()) || {};
    var yearDiff = (Number(metaA.yearBe) || 0) - (Number(metaB.yearBe) || 0);
    if (yearDiff !== 0) return yearDiff;
    var monthA = Number(metaA.month) || 0;
    var monthB = Number(metaB.month) || 0;
    if (monthA !== monthB) return monthA - monthB;
    return String(a.getName() || '').localeCompare(String(b.getName() || ''));
  });

  return sheets;
}

function listAllTransactionArchiveSheets_(ss) {
  var sheets = [];
  var seenNames = {};
  if (!ss) return sheets;

  var archiveMetas = getTransactionArchiveSheetsMetaCached_(ss);
  for (var i = 0; i < archiveMetas.length; i++) {
    pushUniqueTransactionSheet_(sheets, seenNames, archiveMetas[i].sheet);
  }

  sheets.sort(function(a, b) {
    var metaA = parseTransactionsArchiveSheetMeta_(a.getName()) || {};
    var metaB = parseTransactionsArchiveSheetMeta_(b.getName()) || {};
    var yearDiff = (Number(metaA.yearBe) || 0) - (Number(metaB.yearBe) || 0);
    if (yearDiff !== 0) return yearDiff;
    var monthA = Number(metaA.month) || 0;
    var monthB = Number(metaB.month) || 0;
    if (monthA !== monthB) return monthA - monthB;
    return String(a.getName() || '').localeCompare(String(b.getName() || ''));
  });

  return sheets;
}

function getManagedPaymentAvailableYears_(ss) {
  var spreadsheet = ss || getDatabaseSpreadsheet_();
  var cacheKey = 'managedPaymentAvailableYears';
  var cached = getJsonCache_(cacheKey);
  if (cached && cached.years) return cached.years;

  var years = {};
  var currentYearBe = (parseThaiDateParts_(new Date()) || {}).yearBe || (new Date().getFullYear() + 543);
  years[String(currentYearBe)] = true;

  var archiveMetas = getTransactionArchiveSheetsMetaCached_(spreadsheet);
  for (var i = 0; i < archiveMetas.length; i++) {
    var meta = archiveMetas[i].meta || {};
    if (meta && meta.yearBe) years[String(Number(meta.yearBe) || 0)] = true;
  }

  var result = Object.keys(years)
    .map(function(value) { return Number(value) || 0; })
    .filter(function(value) { return value > 0; })
    .sort(function(a, b) { return b - a; });

  putJsonCache_(cacheKey, { years: result }, 300);
  return result;
}

function listManagedPaymentRelevantSheets_(ss, targetYearBe) {
  var spreadsheet = ss || getDatabaseSpreadsheet_();
  var normalizedYearBe = Number(targetYearBe) || ((parseThaiDateParts_(new Date()) || {}).yearBe || (new Date().getFullYear() + 543));
  var currentYearBe = (parseThaiDateParts_(new Date()) || {}).yearBe || (new Date().getFullYear() + 543);
  var sheets = [];
  var seen = {};

  function pushSheet_(sheet) {
    if (!sheet) return;
    var name = String(sheet.getName() || '');
    if (!name || seen[name]) return;
    sheets.push(sheet);
    seen[name] = true;
  }

  if (Number(normalizedYearBe) === Number(currentYearBe)) {
    var currentSheets = listCurrentTransactionSheets_(spreadsheet);
    for (var i = 0; i < currentSheets.length; i++) pushSheet_(currentSheets[i]);
  }

  var archiveSheets = listTransactionArchiveSheetsByYear_(spreadsheet, normalizedYearBe);
  for (var a = 0; a < archiveSheets.length; a++) pushSheet_(archiveSheets[a]);

  return sheets;
}

function findRowsByExactValueInColumn_(sheet, columnIndex, targetValue, startRow) {
  var rows = getExactRowIndexesByColumnValueCached_(sheet, columnIndex, targetValue, startRow);
  return rows.sort(function(a, b) { return a - b; });
}

function readSheetRowsByIndexes_(sheet, rowIndexes, readCols) {
  var result = [];
  if (!sheet || !rowIndexes || rowIndexes.length === 0) return result;

  var normalizedCols = Math.max(1, Number(readCols) || sheet.getLastColumn() || 1);
  var sortedRows = rowIndexes.slice().sort(function(a, b) { return a - b; });
  var ranges = [];
  var startRow = sortedRows[0];
  var prevRow = sortedRows[0];

  for (var i = 1; i < sortedRows.length; i++) {
    var rowIndex = sortedRows[i];
    if (rowIndex === prevRow + 1) {
      prevRow = rowIndex;
      continue;
    }
    ranges.push({ startRow: startRow, rowCount: prevRow - startRow + 1 });
    startRow = rowIndex;
    prevRow = rowIndex;
  }
  ranges.push({ startRow: startRow, rowCount: prevRow - startRow + 1 });

  for (var r = 0; r < ranges.length; r++) {
    var rangeMeta = ranges[r];
    var values = sheet.getRange(rangeMeta.startRow, 1, rangeMeta.rowCount, normalizedCols).getValues();
    for (var offset = 0; offset < values.length; offset++) {
      result.push({ rowIndex: rangeMeta.startRow + offset, values: values[offset] });
    }
  }

  return result;
}

function ensureTransactionsArchiveSheet_(ss, buddhistYear, month) {
  var sheetName = getTransactionsArchiveSheetName_(buddhistYear, month);
  var requiredHeaders = [
    'รหัสอ้างอิง', 'วัน/เดือน/ปี (พ.ศ.)', 'เลขที่สัญญา', 'รหัสสมาชิก',
    'ชำระเงินต้น', 'ชำระดอกเบี้ย', 'ยอดคงเหลือ', 'หมายเหตุ',
    'ผู้ทำรายการ', 'ชื่อ-สกุล', 'สถานะการทำรายการ',
    'จำนวนงวดดอกที่ชำระ', 'ค้างดอกก่อนรับชำระ', 'ค้างดอกหลังรับชำระ',
    'วันที่ปิดยอด', 'รอบปิดยอดเดือน'
  ];

  var existingSheet = ss.getSheetByName(sheetName);
  var sheet = existingSheet || createSheetSafely_(ss, sheetName, 2, requiredHeaders.length, 'เตรียมชีต archive ' + sheetName);
  if (!existingSheet) invalidateSpreadsheetExecutionCaches_(ss);

  ensureSheetGridSizeSafely_(sheet, Math.max(sheet.getMaxRows(), 2), requiredHeaders.length, 'เตรียมชีต archive ' + sheetName);

  if (sheet.getLastRow() === 0) {
    sheet.getRange(1, 1, 1, requiredHeaders.length).setValues([requiredHeaders]).setFontWeight('bold').setBackground('#dbeafe');
    setPlainTextColumnFormat_(sheet, [1, 2, 3, 4, 15, 16]);
    return sheet;
  }

  var headers = sheet.getRange(1, 1, 1, sheet.getLastColumn()).getValues()[0];
  var missingHeaders = [];
  for (var i = 0; i < requiredHeaders.length; i++) {
    if (headers.indexOf(requiredHeaders[i]) === -1) {
      missingHeaders.push(requiredHeaders[i]);
    }
  }

  if (missingHeaders.length > 0) {
    ensureSheetGridSizeSafely_(sheet, sheet.getMaxRows(), sheet.getLastColumn() + missingHeaders.length, 'ขยายคอลัมน์ชีต archive ' + sheetName);
    for (var m = 0; m < missingHeaders.length; m++) {
      sheet.insertColumnAfter(sheet.getLastColumn());
      var lastCol = sheet.getLastColumn();
      sheet.getRange(1, lastCol).setValue(missingHeaders[m]).setFontWeight('bold').setBackground('#dbeafe');
      headers.push(missingHeaders[m]);
    }
  }

  setPlainTextColumnFormat_(sheet, [1, 2, 3, 4, 15, 16]);
  return sheet;
}


function resolveArchivePeriodFromArchivedRow_(rowValues) {
  var row = rowValues || [];
  var txDateParts = parseThaiDateParts_(row[1]);
  if (txDateParts && txDateParts.yearBe) {
    return {
      yearBe: Number(txDateParts.yearBe) || 0,
      month: Math.min(12, Math.max(1, Number(txDateParts.month) || 1))
    };
  }

  var closingMonthKey = String(row[15] || '').trim();
  var closingMatch = closingMonthKey.match(/^(\d{4})-(\d{2})$/);
  if (closingMatch) {
    return {
      yearBe: Number(closingMatch[1]) || 0,
      month: Math.min(12, Math.max(1, Number(closingMatch[2]) || 1))
    };
  }

  var closingDateParts = parseThaiDateParts_(row[14]);
  if (closingDateParts && closingDateParts.yearBe) {
    return {
      yearBe: Number(closingDateParts.yearBe) || 0,
      month: Math.min(12, Math.max(1, Number(closingDateParts.month) || 1))
    };
  }

  return null;
}

function collectInterestInstallmentsForYearFromSheet_(sheet, targetYearBe, collector) {
  if (!sheet || sheet.getLastRow() <= 1) return;

  var readCols = Math.min(sheet.getLastColumn(), 12);
  if (readCols < 6) return;

  var rows = sheet.getRange(2, 1, sheet.getLastRow() - 1, readCols).getValues();
  for (var i = 0; i < rows.length; i++) {
    var row = rows[i];
    var txDate = parseThaiDateParts_(row[1]);
    if (!txDate || txDate.yearBe !== targetYearBe) continue;
    if (isInactiveTransactionStatus_(String(row[10] || '').trim())) continue;

    var contract = String(row[2] || '').trim();
    if (!contract) continue;

    var interestPaid = Number(row[5]) || 0;
    var installments = Number(row[11]) || 0;
    if (interestPaid <= 0 && installments <= 0) continue;

    installments = Math.max(1, installments || 0);
    collector[contract] = (collector[contract] || 0) + installments;
  }
}

function buildEmptyDashboardMonthCollector_(targetYearBe, month) {
  return {
    month: month,
    yearBe: targetYearBe,
    principalPaid: 0,
    offsetPaid: 0,
    interestPaid: 0,
    principalReduction: 0,
    newLoanAmount: 0,
    broughtForwardDebt: 0,
    carriedForwardDebt: 0,
    totalSent: 0,
    totalCollected: 0,
    transactionCount: 0,
    memberCount: 0,
    contractCount: 0,
    _memberSet: {},
    _contractSet: {}
  };
}

function aggregateYearlyTransactionFacts_(ss, targetYearBe, monthLimit) {
  var normalizedYearBe = Number(targetYearBe) || 0;
  var normalizedMonthLimit = Math.min(12, Math.max(1, Number(monthLimit) || 12));
  var emptyResult = {
    paidInstallmentsByContract: {},
    collectorByMonth: {},
    principalReductionByContract: {}
  };
  if (!ss || !normalizedYearBe) return emptyResult;

  var cacheKey = 'yearlyTransactionFacts::' + String(normalizedYearBe) + '::' + String(normalizedMonthLimit);
  var cached = getJsonCache_(cacheKey);
  if (cached && cached.paidInstallmentsByContract && cached.collectorByMonth && cached.principalReductionByContract) {
    return cached;
  }

  var collectorByMonth = {};
  for (var month = 1; month <= normalizedMonthLimit; month++) {
    collectorByMonth[month] = buildEmptyDashboardMonthCollector_(normalizedYearBe, month);
  }

  var principalReductionByContract = {};
  var paidInstallmentsByContract = {};
  var sheetsToScan = listCurrentTransactionSheets_(ss).concat(listTransactionArchiveSheetsByYear_(ss, normalizedYearBe));

  for (var sheetIndex = 0; sheetIndex < sheetsToScan.length; sheetIndex++) {
    var sheet = sheetsToScan[sheetIndex];
    var records = getActiveTransactionRecordsFromSheetCached_(sheet);
    if (!records || !records.length) continue;

    for (var recordIndex = 0; recordIndex < records.length; recordIndex++) {
      var record = records[recordIndex];
      if (Number(record.yearBe) !== normalizedYearBe) continue;

      var contractNo = String(record.contract || '').trim();
      var interestPaid = Number(record.interestPaid) || 0;
      var installments = Number(record.interestMonthsPaid) || 0;

      if (contractNo && (interestPaid > 0 || installments > 0)) {
        paidInstallmentsByContract[contractNo] = (paidInstallmentsByContract[contractNo] || 0) + Math.max(1, installments || 0);
      }

      var month = Number(record.month) || 0;
      if (!month || !collectorByMonth[month]) continue;

      var principalPaid = Number(record.principalPaid) || 0;
      var noteText = String(record.note || '').trim();
      var isOffset = noteText.indexOf('กลบหนี้') !== -1;
      var memberId = String(record.memberId || '').trim();

      if (isOffset) {
        collectorByMonth[month].offsetPaid += principalPaid;
      } else {
        collectorByMonth[month].principalPaid += principalPaid;
      }

      collectorByMonth[month].interestPaid += interestPaid;
      collectorByMonth[month].principalReduction += principalPaid;

      if (contractNo) {
        principalReductionByContract[contractNo] = (principalReductionByContract[contractNo] || 0) + principalPaid;
      }

      if (principalPaid > 0 || interestPaid > 0) {
        collectorByMonth[month].transactionCount += 1;
      }

      if (memberId) collectorByMonth[month]._memberSet[memberId] = true;
      if (contractNo) collectorByMonth[month]._contractSet[contractNo] = true;
    }
  }

  var result = {
    paidInstallmentsByContract: paidInstallmentsByContract,
    collectorByMonth: collectorByMonth,
    principalReductionByContract: principalReductionByContract
  };
  putJsonCache_(cacheKey, result, 300);
  return result;
}

function collectInterestInstallmentsForYearAcrossSources_(ss, targetYearBe) {
  var normalizedYearBe = Number(targetYearBe) || 0;
  if (!ss || !normalizedYearBe) return {};
  return aggregateYearlyTransactionFacts_(ss, normalizedYearBe, 12).paidInstallmentsByContract || {};
}

function aggregateMonthlyCoverSummariesFromSheet_(sheet, targetYearBe, collectorByMonth, principalReductionByContract) {
  if (!sheet || sheet.getLastRow() <= 1) return;

  ensureTransactionsSheetSchema_(sheet);

  var readCols = Math.min(Math.max(sheet.getLastColumn(), 14), 14);
  var rows = sheet.getRange(2, 1, sheet.getLastRow() - 1, readCols).getValues();

  for (var i = 0; i < rows.length; i++) {
    var row = rows[i];
    if (isInactiveTransactionStatus_(String(row[10] || '').trim())) continue;

    var txDate = parseThaiDateParts_(row[1]);
    if (!txDate || Number(txDate.yearBe) !== Number(targetYearBe)) continue;

    var month = Number(txDate.month) || 0;
    if (!month || !collectorByMonth[month]) continue;

    var principalPaid = Number(row[4]) || 0;
    var interestPaid = Number(row[5]) || 0;
    var noteText = String(row[7] || '').trim();
    var isOffset = noteText.indexOf('กลบหนี้') !== -1;
    var memberId = String(row[3] || '').trim();
    var contractNo = String(row[2] || '').trim();

    if (isOffset) {
      collectorByMonth[month].offsetPaid += principalPaid;
    } else {
      collectorByMonth[month].principalPaid += principalPaid;
    }

    collectorByMonth[month].interestPaid += interestPaid;
    collectorByMonth[month].principalReduction += principalPaid;

    if (contractNo) {
      principalReductionByContract[contractNo] = (principalReductionByContract[contractNo] || 0) + principalPaid;
    }

    if (principalPaid > 0 || interestPaid > 0) {
      collectorByMonth[month].transactionCount += 1;
    }

    if (memberId) collectorByMonth[month]._memberSet[memberId] = true;
    if (contractNo) collectorByMonth[month]._contractSet[contractNo] = true;
  }
}

function computeOpeningDebtAndNewLoansForDashboard_(ss, targetYearBe, monthLimit, principalReductionByContract, preloadedLoanRows) {
  var result = {
    openingDebt: 0,
    newLoanAmountByMonth: {}
  };

  for (var month = 1; month <= monthLimit; month++) {
    result.newLoanAmountByMonth[month] = 0;
  }

  if (!ss && !preloadedLoanRows) return result;

  var loanRows = Array.isArray(preloadedLoanRows) ? preloadedLoanRows : [];
  if (!loanRows.length) {
    var loanSheet = ss.getSheetByName('Loans');
    if (!loanSheet || loanSheet.getLastRow() <= 1) return result;
    loanRows = loanSheet.getRange(2, 1, loanSheet.getLastRow() - 1, 12).getValues();
  }

  for (var i = 0; i < loanRows.length; i++) {
    var row = loanRows[i];
    var contractNo = String(row[1] || '').trim();
    var originalAmount = Number(row[3]) || 0;
    var currentBalance = Number(row[5]) || 0;
    var createdParts = parseThaiDateParts_(row[9]);

    if (createdParts && Number(createdParts.yearBe) > Number(targetYearBe)) {
      continue;
    }

    if (createdParts && Number(createdParts.yearBe) === Number(targetYearBe)) {
      var createdMonth = Math.min(12, Math.max(1, Number(createdParts.month) || 1));
      if (result.newLoanAmountByMonth[createdMonth] === undefined) {
        result.newLoanAmountByMonth[createdMonth] = 0;
      }
      result.newLoanAmountByMonth[createdMonth] += originalAmount;
      continue;
    }

    result.openingDebt += currentBalance + (principalReductionByContract[contractNo] || 0);
  }

  return result;
}

function getMonthlyCoverSummariesForDashboard_(ss, asOfDate, preloadedLoanRows) {
  var asOfParts = parseThaiDateParts_(asOfDate || new Date());
  if (!asOfParts) {
    var nativeNow = new Date();
    asOfParts = { month: nativeNow.getMonth() + 1, yearBe: nativeNow.getFullYear() + 543 };
  }

  var targetYearBe = Number(asOfParts.yearBe) || 0;
  var monthLimit = Math.min(Math.max(Number(asOfParts.month) || 1, 1), 12);
  var cacheKey = 'monthlyCoverDashboard::' + String(targetYearBe) + '::' + String(monthLimit);
  var cached = getJsonCache_(cacheKey);
  if (cached) return cached;

  var yearlyFacts = aggregateYearlyTransactionFacts_(ss, targetYearBe, monthLimit);
  var collectorByMonth = yearlyFacts.collectorByMonth || {};
  var principalReductionByContract = yearlyFacts.principalReductionByContract || {};

  for (var month = 1; month <= monthLimit; month++) {
    if (!collectorByMonth[month]) {
      collectorByMonth[month] = buildEmptyDashboardMonthCollector_(targetYearBe, month);
    }
  }

  var debtSeed = computeOpeningDebtAndNewLoansForDashboard_(ss, targetYearBe, monthLimit, principalReductionByContract, preloadedLoanRows);
  var runningDebt = Math.max(0, Number(debtSeed.openingDebt) || 0);

  var summaries = [];
  for (var currentMonth = 1; currentMonth <= monthLimit; currentMonth++) {
    var item = collectorByMonth[currentMonth];
    item.newLoanAmount = Math.max(0, Number(debtSeed.newLoanAmountByMonth[currentMonth]) || 0);
    item.broughtForwardDebt = runningDebt;
    item.totalSent = item.principalPaid + item.interestPaid;
    item.totalCollected = item.principalPaid + item.interestPaid + item.offsetPaid;
    runningDebt = Math.max(0, runningDebt + item.newLoanAmount - item.principalReduction);
    item.carriedForwardDebt = runningDebt;
    item.memberCount = Object.keys(item._memberSet || {}).length;
    item.contractCount = Object.keys(item._contractSet || {}).length;
    delete item._memberSet;
    delete item._contractSet;
    summaries.push(item);
  }

  var result = {
    yearBe: targetYearBe,
    monthLimit: monthLimit,
    cards: summaries
  };
  putJsonCache_(cacheKey, result, 300);
  return result;
}

function getCurrentThaiMonthContext_() {
  var nowParts = parseThaiDateParts_(new Date()) || { month: new Date().getMonth() + 1, yearBe: new Date().getFullYear() + 543 };
  return {
    month: nowParts.month,
    yearBe: nowParts.yearBe,
    monthKey: nowParts.yearBe + '-' + padNumber_(nowParts.month, 2)
  };
}

function shouldCountSelectedMonthAsDueForOverdueReport_(reportYearBe, reportMonth, lastClosedAccountingMonthKey) {
  var safeYearBe = Number(reportYearBe) || 0;
  var safeMonth = Math.min(12, Math.max(1, Number(reportMonth) || 1));
  var selectedMonthKey = String(safeYearBe) + '-' + padNumber_(safeMonth, 2);
  var closedMonthKey = String(lastClosedAccountingMonthKey || '').trim();

  if (/^\d{4}-\d{2}$/.test(closedMonthKey)) {
    return selectedMonthKey <= closedMonthKey;
  }

  var currentContext = getCurrentThaiMonthContext_();
  var currentMonthKey = String((currentContext && currentContext.monthKey) || '').trim();
  if (!currentMonthKey) return true;
  return selectedMonthKey < currentMonthKey;
}

function calculateLoanAnnualInterestState_(loanCreatedAtValue, paidInstallmentsYtd, asOfDate, currentBalance, currentStatus) {
  if (shouldExcludeLoanFromArrearsCount_(currentBalance, currentStatus)) {
    var nowForClosedLoan = asOfDate || new Date();
    var closedParts = parseThaiDateParts_(nowForClosedLoan) || { month: nowForClosedLoan.getMonth() + 1, yearBe: nowForClosedLoan.getFullYear() + 543 };
    return {
      yearBe: Number(closedParts.yearBe) || (nowForClosedLoan.getFullYear() + 543),
      startMonth: 13,
      totalSchedulableMonthsInYear: 0,
      dueMonthsYtd: 0,
      paidInstallmentsYtd: Math.max(0, Number(paidInstallmentsYtd) || 0),
      currentMonthCovered: true,
      missedInterestMonths: 0,
      maxAllowedPaymentInstallments: 0
    };
  }

  var asOfParts = parseThaiDateParts_(asOfDate || new Date());
  if (!asOfParts) {
    var nativeDate = new Date();
    asOfParts = { month: nativeDate.getMonth() + 1, yearBe: nativeDate.getFullYear() + 543 };
  }

  var currentYearBe = Number(asOfParts.yearBe) || 0;
  var currentMonth = Number(asOfParts.month) || 1;
  var createdParts = parseThaiDateParts_(loanCreatedAtValue);

  if (createdParts && createdParts.yearBe > currentYearBe) {
    return {
      yearBe: currentYearBe,
      startMonth: 13,
      totalSchedulableMonthsInYear: 0,
      dueMonthsYtd: 0,
      paidInstallmentsYtd: 0,
      currentMonthCovered: false,
      missedInterestMonths: 0,
      maxAllowedPaymentInstallments: 0
    };
  }

  var startMonth = 1;
  if (createdParts && createdParts.yearBe === currentYearBe) {
    startMonth = Math.min(12, Math.max(1, Number(createdParts.month) || 1));
  }

  var totalSchedulableMonths = Math.max(0, 12 - startMonth + 1);
  var monthsScheduledThroughCurrent = currentMonth >= startMonth ? (currentMonth - startMonth + 1) : 0;
  var dueMonthsYtd = Math.min(totalSchedulableMonths, Math.max(0, monthsScheduledThroughCurrent - 1));
  var normalizedPaidInstallments = Math.max(0, Number(paidInstallmentsYtd) || 0);
  var cappedPaidInstallments = Math.min(totalSchedulableMonths, normalizedPaidInstallments);
  var currentMonthCovered = monthsScheduledThroughCurrent > 0 && cappedPaidInstallments >= monthsScheduledThroughCurrent;
  var missedInterestMonths = Math.max(0, dueMonthsYtd - cappedPaidInstallments);
  var remainingInstallmentsThisYear = Math.max(0, totalSchedulableMonths - cappedPaidInstallments);

  return {
    yearBe: currentYearBe,
    startMonth: startMonth,
    totalSchedulableMonthsInYear: totalSchedulableMonths,
    dueMonthsYtd: dueMonthsYtd,
    paidInstallmentsYtd: cappedPaidInstallments,
    currentMonthCovered: currentMonthCovered,
    missedInterestMonths: missedInterestMonths,
    maxAllowedPaymentInstallments: Math.max(0, remainingInstallmentsThisYear)
  };
}

function buildLoanAnnualInterestStateIndexFromRows_(loanRows, paidInstallmentsByContract, asOfDate) {
  var index = {};
  var safeRows = loanRows || [];
  var paidIndex = paidInstallmentsByContract || {};

  for (var i = 0; i < safeRows.length; i++) {
    var row = safeRows[i];
    var contractNo = String(row[1] || '').trim();
    if (!contractNo) continue;
    index[contractNo] = calculateLoanAnnualInterestState_(row[9], paidIndex[contractNo], asOfDate, row[5], row[6]);
  }

  return index;
}

function getInterestInstallmentsPaidForContractInYear_(ss, contractNo, targetYearBe) {
  var paidInstallmentsByContract = collectInterestInstallmentsForYearAcrossSources_(ss, targetYearBe);
  return Math.max(0, Number(paidInstallmentsByContract[String(contractNo || '').trim()] || 0));
}

function recalculateAnnualInterestArrearsSnapshots_(ss, asOfDate, options) {
  var recalcOptions = options || {};
  var includeCurrentMonthInstallment = !!recalcOptions.includeCurrentMonthInstallment;
  var spreadsheet = ss || getDatabaseSpreadsheet_();
  var loanSheet = getSheetOrThrow(spreadsheet, 'Loans');
  if (loanSheet.getLastRow() <= 1) {
    clearAppCache_();
    return {
      status: 'Success',
      processedLoans: 0,
      changedLoans: 0,
      targetYearBe: (parseThaiDateParts_(asOfDate || new Date()) || {}).yearBe || (new Date().getFullYear() + 543),
      totalArrearsLoans: 0,
      totalPaidInstallmentsYtd: 0
    };
  }

  var asOfParts = parseThaiDateParts_(asOfDate || new Date()) || { yearBe: new Date().getFullYear() + 543 };
  var targetYearBe = Number(asOfParts.yearBe) || (new Date().getFullYear() + 543);
  var rowCount = loanSheet.getLastRow() - 1;
  var loanRows = loanSheet.getRange(2, 1, rowCount, 12).getValues();
  var paidInstallmentsByContract = collectInterestInstallmentsForYearAcrossSources_(spreadsheet, targetYearBe);
  var stateIndex = buildLoanAnnualInterestStateIndexFromRows_(loanRows, paidInstallmentsByContract, asOfDate);
  var changedUpdates = [];
  var changedLoans = 0;
  var totalArrearsLoans = 0;
  var totalPaidInstallmentsYtd = 0;

  for (var i = 0; i < loanRows.length; i++) {
    var row = loanRows[i];
    var contractNo = String(row[1] || '').trim();
    var currentStatus = String(row[6] || '').trim();
    var currentNextPayment = row[7];
    var currentMissed = Math.max(0, Number(row[8]) || 0);
    var annualState = stateIndex[contractNo] || calculateLoanAnnualInterestState_(row[9], 0, asOfDate, row[5], row[6]);
    var excludedFromArrears = shouldExcludeLoanFromArrearsCount_(row[5], currentStatus);
    var currentMonthInstallment = includeCurrentMonthInstallment && !annualState.currentMonthCovered ? 1 : 0;
    var newMissed = excludedFromArrears ? 0 : (Math.max(0, Number(annualState.missedInterestMonths) || 0) + currentMonthInstallment);
    var newStatus = deriveLoanStatusFromState_(Number(row[5]) || 0, newMissed, currentStatus || 'ปกติ');

    if (newMissed > 0) totalArrearsLoans += 1;
    totalPaidInstallmentsYtd += Math.max(0, Number(annualState.paidInstallmentsYtd) || 0);

    if (newMissed !== currentMissed || newStatus !== currentStatus) {
      changedLoans += 1;
      changedUpdates.push({
        rowIndex: i + 2,
        values: [newStatus, currentNextPayment, newMissed]
      });
    }
  }

  if (changedUpdates.length > 0) {
    writeSheetPartialRowsBatchByIndexes_(loanSheet, 7, 3, changedUpdates);
  }

  var settingsSheet = spreadsheet.getSheetByName('Settings') || createSheetSafely_(spreadsheet, 'Settings', 3, 2, 'เตรียมชีต Settings');
  upsertSettingValue_(settingsSheet, 'LastAnnualArrearsRecalcAt', getThaiDate(asOfDate || new Date()).dateTime);
  upsertSettingValue_(settingsSheet, 'LastAnnualArrearsRecalcYear', targetYearBe);

  clearAppCache_();
  return {
    status: 'Success',
    processedLoans: rowCount,
    changedLoans: changedLoans,
    targetYearBe: targetYearBe,
    totalArrearsLoans: totalArrearsLoans,
    totalPaidInstallmentsYtd: totalPaidInstallmentsYtd
  };
}

var DAILY_INTEREST_ARREARS_TRIGGER_HANDLER_ = 'runScheduledDailyInterestArrearsRefresh';

function getDailyInterestArrearsTriggerStatus_() {
  return {
    installed: false,
    count: 0,
    handler: DAILY_INTEREST_ARREARS_TRIGGER_HANDLER_,
    scheduleLabel: 'ปิดการทำงานอัตโนมัติแล้ว (ใช้ปุ่มปิดยอดสิ้นวันเท่านั้น)'
  };
}

function disableDailyInterestArrearsTriggersIfAny_() {
  return {
    status: 'Success',
    removedCount: 0,
    installed: false,
    scheduleLabel: 'ปิดการทำงานอัตโนมัติแล้ว (ใช้ปุ่มปิดยอดสิ้นวันเท่านั้น)'
  };
}

function ensureDailyInterestArrearsTriggerIfMissing_() {
  return getDailyInterestArrearsTriggerStatus_();
}

function installDailyInterestArrearsTrigger() {
  requireAdminSession_();
  var ss = getDatabaseSpreadsheet_();
  var settingsSheet = ss.getSheetByName('Settings') || createSheetSafely_(ss, 'Settings', 3, 2, 'เตรียมชีต Settings');
  upsertSettingValue_(settingsSheet, 'DailyInterestArrearsTriggerSchedule', 'DISABLED');
  upsertSettingValue_(settingsSheet, 'DailyInterestArrearsTriggerInstalledAt', '');
  clearAppCache_();
  return {
    status: 'Success',
    message: 'ระบบปิดการคำนวณค้างดอกอัตโนมัติแล้ว ใช้ปุ่มปิดยอดสิ้นวันเท่านั้น',
    automation: getDailyInterestArrearsTriggerStatus_()
  };
}

function runScheduledDailyInterestArrearsRefresh() {
  requireAdminSession_();
  clearAppCache_();
  return {
    status: 'Success',
    message: 'ระบบปิดการคำนวณค้างดอกอัตโนมัติแล้ว ใช้ปุ่มปิดยอดสิ้นวันเท่านั้น'
  };
}

function deleteRowsByIndexes_(sheet, rowIndexes) {
  if (!sheet || !rowIndexes || rowIndexes.length === 0) return;
  invalidateSheetExecutionCaches_(sheet);

  var sorted = rowIndexes.slice().sort(function(a, b) { return a - b; });
  var lastRow = sheet.getLastRow();
  var clearCols = Math.max(1, sheet.getLastColumn() || 1);

  if (
    lastRow > 1 &&
    sorted[0] === 2 &&
    sorted[sorted.length - 1] === lastRow &&
    sorted.length === Math.max(0, lastRow - 1)
  ) {
    sheet.getRange(2, 1, lastRow - 1, clearCols).clearContent();
    invalidateSheetExecutionCaches_(sheet);
    return;
  }

  var groups = [];
  var start = sorted[0];
  var prev = sorted[0];

  for (var i = 1; i < sorted.length; i++) {
    if (sorted[i] === prev + 1) {
      prev = sorted[i];
      continue;
    }
    groups.push({ start: start, end: prev });
    start = sorted[i];
    prev = sorted[i];
  }
  groups.push({ start: start, end: prev });

  for (var j = groups.length - 1; j >= 0; j--) {
    var group = groups[j];
    if (group.start === 2 && group.end === lastRow && (group.end - group.start + 1) >= Math.max(1, lastRow - 1)) {
      sheet.getRange(2, 1, lastRow - 1, clearCols).clearContent();
      invalidateSheetExecutionCaches_(sheet);
      return;
    }
    sheet.deleteRows(group.start, group.end - group.start + 1);
  }
  invalidateSheetExecutionCaches_(sheet);
}

function parseReportDateInput_(value) {
  if (value === null || value === undefined || value === '') return null;

  var directParts = parseThaiDateParts_(value);
  if (directParts) return directParts;

  var text = String(value || '').trim();
  if (!text) return null;

  var isoMatch = text.match(/^(\d{4})-(\d{1,2})-(\d{1,2})$/);
  if (isoMatch) {
    var yearAd = Number(isoMatch[1]) || 0;
    var month = Number(isoMatch[2]) || 0;
    var day = Number(isoMatch[3]) || 0;
    if (yearAd && month && day) {
      return {
        day: day,
        month: month,
        yearBe: yearAd + 543,
        dateOnly: padNumber_(day, 2) + '/' + padNumber_(month, 2) + '/' + (yearAd + 543)
      };
    }
  }

  var nativeDate = new Date(text);
  if (!isNaN(nativeDate.getTime())) {
    return parseThaiDateParts_(nativeDate);
  }

  return null;
}

function getThaiDateNumberFromParts_(parts) {
  if (!parts) return 0;
  return Number(String(parts.yearBe || '') + padNumber_(parts.month || 0, 2) + padNumber_(parts.day || 0, 2)) || 0;
}

function getThaiDateNumber_(value) {
  var parts = parseThaiDateParts_(value);
  return getThaiDateNumberFromParts_(parts);
}

function listRelevantTransactionSheetsForReport_(ss, reportYearBe) {
  var sheets = [];
  var seenNames = {};
  var normalizedReportYearBe = Number(reportYearBe) || 0;

  function pushSheet_(sheet) {
    if (!sheet) return;
    var name = String(sheet.getName() || '');
    if (!name || seenNames[name]) return;
    sheets.push(sheet);
    seenNames[name] = true;
  }

  var currentSheets = listCurrentTransactionSheets_(ss);
  for (var i = 0; i < currentSheets.length; i++) pushSheet_(currentSheets[i]);

  var archiveSheets = normalizedReportYearBe
    ? listTransactionArchiveSheetsFromYearForward_(ss, normalizedReportYearBe)
    : listAllTransactionArchiveSheets_(ss);

  for (var a = 0; a < archiveSheets.length; a++) pushSheet_(archiveSheets[a]);

  return sheets;
}

function listTransactionArchiveSheetsFromYearForward_(ss, startYearBe) {
  var normalizedStartYearBe = Number(startYearBe) || 0;
  if (!ss || !normalizedStartYearBe) {
    return listAllTransactionArchiveSheets_(ss);
  }

  var metaEntries = getTransactionArchiveSheetsMetaCached_(ss);
  var result = [];
  for (var i = 0; i < metaEntries.length; i++) {
    var entry = metaEntries[i];
    var meta = entry && entry.meta ? entry.meta : null;
    if (!meta || !entry.sheet) continue;
    var entryYearBe = Number(meta.yearBe) || 0;
    if (!entryYearBe || entryYearBe < normalizedStartYearBe) continue;
    result.push(entry.sheet);
  }

  return result;
}

function compareTransactionRecordOrderAsc_(a, b) {
  var dateDiff = (Number((a && a.dateNumber) || 0) - Number((b && b.dateNumber) || 0));
  if (dateDiff !== 0) return dateDiff;
  return String((a && a.timestamp) || '').localeCompare(String((b && b.timestamp) || ''));
}

function getActiveTransactionRecordsFromSheetCached_(sheet) {
  if (!sheet || sheet.getLastRow() <= 1) return [];

  ensureTransactionsSheetSchema_(sheet);

  var readCols = Math.min(Math.max(sheet.getLastColumn(), 14), 16);
  var cacheKey = getSheetExecutionCacheKey_(sheet, -readCols, 2) + '::activeTx';
  if (EXECUTION_ACTIVE_TRANSACTION_RECORDS_CACHE_.hasOwnProperty(cacheKey)) {
    return EXECUTION_ACTIVE_TRANSACTION_RECORDS_CACHE_[cacheKey];
  }

  var rows = sheet.getRange(2, 1, sheet.getLastRow() - 1, readCols).getValues();
  var sourceSheetName = String(sheet.getName() || '');
  var records = [];

  for (var i = 0; i < rows.length; i++) {
    var row = rows[i];
    if (isInactiveTransactionStatus_(String(row[10] || '').trim())) continue;

    var dateParts = parseThaiDateParts_(row[1]);
    if (!dateParts) continue;

    records.push({
      id: String(row[0] || '').trim(),
      timestamp: normalizeThaiDateCellValue_(row[1], true),
      contract: String(row[2] || '').trim(),
      memberId: String(row[3] || '').trim(),
      principalPaid: Number(row[4]) || 0,
      interestPaid: Number(row[5]) || 0,
      newBalance: Number(row[6]) || 0,
      note: String(row[7] || '').trim(),
      actorName: String(row[8] || '').trim(),
      memberName: String(row[9] || '').trim(),
      txStatus: String(row[10] || '').trim() || 'ปกติ',
      interestMonthsPaid: Math.max(0, Number(row[11]) || 0),
      missedBeforePayment: Math.max(0, Number(row[12]) || 0),
      missedAfterPayment: Math.max(0, Number(row[13]) || 0),
      dateOnly: dateParts.dateOnly,
      dateNumber: getThaiDateNumberFromParts_(dateParts),
      yearBe: Number(dateParts.yearBe) || 0,
      month: Number(dateParts.month) || 0,
      sourceSheet: sourceSheetName,
      rowIndex: i + 2
    });
  }

  EXECUTION_ACTIVE_TRANSACTION_RECORDS_CACHE_[cacheKey] = records;
  return records;
}

function collectTransactionsForReport_(sheet, collector, seenIds) {
  if (!sheet || sheet.getLastRow() <= 1) return;

  var records = getActiveTransactionRecordsFromSheetCached_(sheet);
  for (var i = 0; i < records.length; i++) {
    var record = records[i];
    var dedupeKey = String(record.id || '').trim() || [
      record.timestamp,
      record.contract,
      record.memberId,
      record.principalPaid,
      record.interestPaid,
      record.newBalance
    ].join('|');
    if (seenIds[dedupeKey]) continue;
    seenIds[dedupeKey] = true;

    collector.push({
      id: record.id,
      timestamp: record.timestamp,
      contract: record.contract,
      memberId: record.memberId,
      principalPaid: record.principalPaid,
      interestPaid: record.interestPaid,
      newBalance: record.newBalance,
      note: record.note,
      memberName: record.memberName,
      dateOnly: record.dateOnly,
      dateNumber: record.dateNumber
    });
  }
}

function listRelevantTransactionSheetsForRegisterReport_(ss, targetYearBe) {
  var sheets = [];
  var seenNames = {};

  function pushSheet_(sheet) {
    if (!sheet) return;
    var name = String(sheet.getName() || '');
    if (!name || seenNames[name]) return;
    sheets.push(sheet);
    seenNames[name] = true;
  }

  var currentSheets = listCurrentTransactionSheets_(ss);
  for (var i = 0; i < currentSheets.length; i++) pushSheet_(currentSheets[i]);

  if (targetYearBe) {
    var archiveSheets = listTransactionArchiveSheetsByYear_(ss, targetYearBe);
    for (var a = 0; a < archiveSheets.length; a++) pushSheet_(archiveSheets[a]);
  }

  return sheets;
}

function collectRegisterTransactionsForMemberFromSheet_(sheet, normalizedMemberId, targetYearBe, collector, seenKeys) {
  if (!sheet || sheet.getLastRow() <= 1) return;

  var records = getActiveTransactionRecordsFromSheetCached_(sheet);
  for (var i = 0; i < records.length; i++) {
    var record = records[i];
    if (normalizeTextForMatch_(record.memberId) !== normalizedMemberId) continue;
    if (Number(targetYearBe) && Number(record.yearBe) !== Number(targetYearBe)) continue;

    var dedupeKey = String(record.id || '').trim() || [
      record.timestamp,
      record.contract,
      record.memberId,
      record.principalPaid,
      record.interestPaid,
      record.newBalance,
      record.note
    ].join('|');

    if (seenKeys[dedupeKey]) continue;
    seenKeys[dedupeKey] = true;

    collector.push({
      id: record.id,
      timestamp: record.timestamp,
      contract: record.contract,
      memberId: record.memberId,
      principalPaid: record.principalPaid,
      interestPaid: record.interestPaid,
      newBalance: record.newBalance,
      note: record.note,
      memberName: record.memberName,
      dateOnly: record.dateOnly,
      dateNumber: record.dateNumber
    });
  }
}



function collectManagedPaymentRecordsFromSheet_(sheet, collector, seenKeys, filterYearBe) {
  if (!sheet || sheet.getLastRow() <= 1) return;

  var records = getActiveTransactionRecordsFromSheetCached_(sheet);

  for (var i = 0; i < records.length; i++) {
    var record = records[i];
    if (Number(filterYearBe) && Number(record.yearBe) !== Number(filterYearBe)) continue;

    var dedupeKey = String(record.id || '').trim() || [
      record.timestamp,
      record.contract,
      record.memberId,
      record.principalPaid,
      record.interestPaid,
      record.newBalance,
      record.note,
      record.sourceSheet,
      String(record.rowIndex || 0)
    ].join('|');

    if (seenKeys[dedupeKey]) continue;
    seenKeys[dedupeKey] = true;

    collector.push({
      id: record.id,
      timestamp: record.timestamp,
      contract: record.contract,
      memberId: record.memberId,
      principalPaid: record.principalPaid,
      interestPaid: record.interestPaid,
      newBalance: record.newBalance,
      note: record.note,
      memberName: record.memberName,
      interestMonthsPaid: record.interestMonthsPaid,
      missedBeforePayment: record.missedBeforePayment,
      missedAfterPayment: record.missedAfterPayment,
      dateOnly: record.dateOnly,
      dateNumber: record.dateNumber,
      sourceSheet: record.sourceSheet,
      rowIndex: record.rowIndex
    });
  }
}

function getManagedPaymentRecords(optionsStr) {
  requirePermission_('payment_records.view', 'ไม่มีสิทธิ์ดูเมนูจัดการรายการรับชำระ');
  try {
    var options = typeof optionsStr === 'string' && optionsStr ? JSON.parse(optionsStr) : (optionsStr || {});
    var ss = getDatabaseSpreadsheet_();
    var currentYearBe = (parseThaiDateParts_(new Date()) || {}).yearBe || (new Date().getFullYear() + 543);
    var targetYearBe = Number(options.yearBe) || currentYearBe;
    var cacheKey = 'managedPaymentRecords::' + String(targetYearBe);
    var cached = getJsonCache_(cacheKey);
    if (cached && cached.records) {
      return {
        status: 'Success',
        records: cached.records,
        availableYears: cached.availableYears || getManagedPaymentAvailableYears_(ss),
        selectedYearBe: targetYearBe,
        cached: true,
        indexed: true
      };
    }

    var indexSheet = ensureFreshTransactionIndexSheet_(ss);
    var records = [];
    if (indexSheet && indexSheet.getLastRow() > 1) {
      var matchedRows = findRowsByExactValueInColumn_(indexSheet, 2, String(targetYearBe), 2);
      var rowMetas = readSheetRowsByIndexes_(indexSheet, matchedRows, TRANSACTION_INDEX_HEADERS.length);
      for (var i = 0; i < rowMetas.length; i++) {
        var record = buildManagedPaymentRecordFromIndexValues_(rowMetas[i].values);
        if (isInactiveTransactionStatus_(record.txStatus)) continue;
        records.push(record);
      }
    }

    records.sort(function(a, b) {
      var keyA = String(a.sortKey || '');
      var keyB = String(b.sortKey || '');
      return keyB.localeCompare(keyA);
    });

    var availableYears = getManagedPaymentAvailableYears_(ss);
    putJsonCache_(cacheKey, { records: records, availableYears: availableYears }, 120);
    return {
      status: 'Success',
      records: records,
      availableYears: availableYears,
      selectedYearBe: targetYearBe,
      cached: false,
      indexed: true
    };
  } catch (e) {
    return {
      status: 'Error',
      message: 'โหลดรายการรับชำระไม่สำเร็จ: ' + e.toString()
    };
  }
}

function normalizeManagedPaymentPayload_(payloadStr) {
  var payload = typeof payloadStr === 'string' ? JSON.parse(payloadStr) : (payloadStr || {});
  var normalizedTimestamp = normalizeThaiDateCellValue_(payload.timestamp, true);
  return {
    id: String(payload.id || '').trim(),
    timestamp: normalizedTimestamp || String(payload.timestamp || '').trim(),
    contract: String(payload.contract || '').trim(),
    memberId: String(payload.memberId || '').trim(),
    memberName: String(payload.memberName || '').trim(),
    principalPaid: Math.max(0, Number(payload.principalPaid) || 0),
    interestPaid: Math.max(0, Number(payload.interestPaid) || 0),
    interestMonthsPaid: Math.max(0, parseInt(payload.interestMonthsPaid, 10) || 0),
    note: sanitizeManagedPaymentNote_(payload.note)
  };
}

function buildManagedTransactionModelFromValues_(sheet, rowIndex, rowValues) {
  var safeValues = (rowValues || []).slice();
  while (safeValues.length < 16) safeValues.push('');

  return {
    txId: String(safeValues[0] || '').trim(),
    timestamp: normalizeThaiDateCellValue_(safeValues[1], true),
    contract: String(safeValues[2] || '').trim(),
    memberId: String(safeValues[3] || '').trim(),
    principalPaid: Math.max(0, Number(safeValues[4]) || 0),
    interestPaid: Math.max(0, Number(safeValues[5]) || 0),
    note: String(safeValues[7] || '').trim(),
    actorName: String(safeValues[8] || '').trim(),
    memberName: String(safeValues[9] || '').trim(),
    txStatus: String(safeValues[10] || '').trim() || 'ปกติ',
    interestMonthsPaid: Math.max(0, parseInt(safeValues[11], 10) || 0),
    sourceSheetName: String(sheet.getName() || ''),
    sourceRowIndex: rowIndex,
    sourceSheet: sheet,
    archiveTail: [safeValues[14] || '', safeValues[15] || '']
  };
}

function buildManagedTransactionModelFromPayload_(matchedTx, payload) {
  var fallbackValues = matchedTx && matchedTx.values ? matchedTx.values : [];
  while (fallbackValues.length < 16) fallbackValues.push('');

  return {
    txId: String(payload.id || fallbackValues[0] || '').trim(),
    timestamp: normalizeThaiDateCellValue_(payload.timestamp || fallbackValues[1], true),
    contract: String(payload.contract || fallbackValues[2] || '').trim(),
    memberId: String(payload.memberId || fallbackValues[3] || '').trim(),
    principalPaid: Math.max(0, Number(payload.principalPaid) || 0),
    interestPaid: Math.max(0, Number(payload.interestPaid) || 0),
    note: String(payload.note || '').trim(),
    actorName: String(fallbackValues[8] || '').trim(),
    memberName: String(payload.memberName || fallbackValues[9] || '').trim(),
    txStatus: String(fallbackValues[10] || '').trim() || 'ปกติ',
    interestMonthsPaid: Math.max(0, parseInt(payload.interestMonthsPaid, 10) || 0),
    sourceSheetName: matchedTx && matchedTx.sheet ? String(matchedTx.sheet.getName() || '') : 'Transactions',
    sourceRowIndex: matchedTx && matchedTx.rowIndex ? matchedTx.rowIndex : 0,
    sourceSheet: matchedTx ? matchedTx.sheet : null,
    archiveTail: [
      fallbackValues[14] || '',
      fallbackValues[15] || ''
    ]
  };
}

function sortManagedTransactionModels_(records) {
  records.sort(function(a, b) {
    var keyA = buildTransactionOrderKey_(['', a.timestamp], a.sourceRowIndex || 0) + '|' + String(a.sourceSheetName || '') + '|' + String(a.txId || '');
    var keyB = buildTransactionOrderKey_(['', b.timestamp], b.sourceRowIndex || 0) + '|' + String(b.sourceSheetName || '') + '|' + String(b.txId || '');
    return keyA.localeCompare(keyB);
  });
}

function getLoanStatePreviewAsOfNow_(loanRow, records) {
  var originalBalance = Math.max(0, Number(loanRow[3]) || 0);
  var finalBalance = originalBalance;
  var lastNote = '';
  var paidByYear = {};

  for (var i = 0; i < records.length; i++) {
    finalBalance = Math.max(0, Number(records[i].computedNewBalance) || 0);
    lastNote = String(records[i].computedNote || '').trim();
    var txParts = parseThaiDateParts_(records[i].timestamp);
    var yearBe = txParts ? Number(txParts.yearBe) || 0 : 0;
    if (yearBe && Number(records[i].computedInterestMonthsPaid) > 0) {
      paidByYear[yearBe] = (paidByYear[yearBe] || 0) + Number(records[i].computedInterestMonthsPaid);
    }
  }

  var now = new Date();
  var nowParts = parseThaiDateParts_(now) || { yearBe: now.getFullYear() + 543 };
  var currentYearBe = Number(nowParts.yearBe) || (now.getFullYear() + 543);
  var annualStateNow = calculateLoanAnnualInterestState_(
    loanRow[9],
    paidByYear[currentYearBe] || 0,
    now,
    finalBalance,
    finalBalance <= 0 && lastNote.indexOf('กลบหนี้') !== -1 ? 'ปิดบัญชี(กลบหนี้)' : 'ปกติ'
  );

  var fallbackStatus = finalBalance <= 0 && lastNote.indexOf('กลบหนี้') !== -1 ? 'ปิดบัญชี(กลบหนี้)' : 'ปกติ';
  var finalMissed = shouldExcludeLoanFromArrearsCount_(finalBalance, fallbackStatus) ? 0 : Math.max(0, Number(annualStateNow.missedInterestMonths) || 0);
  var finalStatus = deriveLoanStatusFromState_(finalBalance, finalMissed, fallbackStatus);

  return {
    balance: finalBalance,
    status: finalStatus,
    missedInterestMonths: finalMissed
  };
}

function determineManagedTargetSheetName_(sourceSheetName, timestamp) {
  var normalizedSource = String(sourceSheetName || '').trim();
  if (normalizedSource === 'Transactions' || !normalizedSource) return 'Transactions';

  var sourceMeta = parseTransactionsArchiveSheetMeta_(normalizedSource);
  if (sourceMeta) {
    var parsed = parseThaiDateParts_(timestamp);
    if (parsed && parsed.yearBe) {
      return getTransactionsArchiveSheetName_(parsed.yearBe, parsed.month);
    }
  }

  return normalizedSource || 'Transactions';
}

function buildManagedWritableValuesForSheet_(sheetName, baseValues, archiveTail) {
  var safeBase = (baseValues || []).slice(0, 14);
  while (safeBase.length < 14) safeBase.push('');

  if (!parseTransactionsArchiveSheetMeta_(String(sheetName || ''))) {
    return safeBase;
  }

  var safeTail = archiveTail || [];
  return safeBase.concat([
    safeTail[0] || '',
    safeTail[1] || ''
  ]);
}

function getManagedTargetSheetForName_(ss, sheetName) {
  if (String(sheetName || '') === 'Transactions' || !sheetName) {
    return resolveWritableTransactionsSheet_(ss);
  }

  var meta = parseTransactionsArchiveSheetMeta_(sheetName);
  if (meta) {
    return ensureTransactionsArchiveSheet_(ss, meta.yearBe, meta.month);
  }

  var sheet = ss.getSheetByName(sheetName);
  if (sheet) return sheet;
  return resolveWritableTransactionsSheet_(ss);
}

function buildManagedContractRecalculationPlan_(ss, contractNo, options) {
  var normalizedContract = String(contractNo || '').trim();
  if (!normalizedContract) {
    throw new Error('ไม่พบเลขที่สัญญาสำหรับคำนวณยอดใหม่');
  }

  var opts = options || {};
  var loanSheet = getSheetOrThrow(ss, 'Loans');
  var loanRowIndex = findRowByExactValue(loanSheet, 2, normalizedContract);
  if (!loanRowIndex) {
    throw new Error('ไม่พบเลขที่สัญญา: ' + normalizedContract);
  }

  var loanRow = loanSheet.getRange(loanRowIndex, 1, 1, 12).getValues()[0];
  var authoritativeMemberId = String(loanRow[0] || '').trim();
  var authoritativeMemberName = String(loanRow[2] || '').trim();
  var annualInterestRate = Number(loanRow[4]);
  if (!isFinite(annualInterestRate) || annualInterestRate < 0) annualInterestRate = 12;
  var runningBalance = Math.max(0, Number(loanRow[3]) || 0);
  var runningStatus = 'ปกติ';
  var targetTxId = String(opts.targetTxId || '').trim();
  var targetMode = String(opts.targetMode || '').trim();
  var replacementMatch = opts.replacementMatch || null;
  var replacementPayload = opts.replacementPayload || null;
  var records = [];

  var matchedRows = getIndexedContractTransactionRows_(ss, normalizedContract);
  for (var r = 0; r < matchedRows.length; r++) {
    var rowMeta = matchedRows[r];
    var row = rowMeta.values;
    if (isInactiveTransactionStatus_(String(row[10] || '').trim())) continue;

    var rowTxId = String(row[0] || '').trim();
    if (targetTxId && rowTxId === targetTxId) {
      if (targetMode === 'remove') {
        continue;
      }
      if (targetMode === 'replace') {
        records.push(buildManagedTransactionModelFromPayload_(replacementMatch, replacementPayload));
        continue;
      }
    }

    records.push(buildManagedTransactionModelFromValues_(rowMeta.sheet, rowMeta.rowIndex, row));
  }

  if (targetMode === 'inject' && replacementMatch && replacementPayload) {
    var injectedRecord = buildManagedTransactionModelFromPayload_(replacementMatch, replacementPayload);
    injectedRecord.sourceSheetName = '';
    injectedRecord.sourceRowIndex = 0;
    injectedRecord.sourceSheet = null;
    records.push(injectedRecord);
  }

  sortManagedTransactionModels_(records);

  var paidInstallmentsByYear = {};
  for (var idx = 0; idx < records.length; idx++) {
    var record = records[idx];
    var txParts = parseThaiDateParts_(record.timestamp);
    if (!txParts) {
      throw new Error('รูปแบบวันเวลาของรายการไม่ถูกต้อง: ' + (record.timestamp || '(ว่าง)'));
    }

    var txYearBe = Number(txParts.yearBe) || 0;
    var paidInstallmentsBefore = paidInstallmentsByYear[txYearBe] || 0;
    var annualStateBefore = calculateLoanAnnualInterestState_(loanRow[9], paidInstallmentsBefore, record.timestamp, runningBalance, runningStatus);
    var normalizedNote = String(record.note || '').trim();
    var isOffsetPayment = normalizedNote.indexOf('กลบหนี้') !== -1;
    var principalPaid = Math.max(0, Number(record.principalPaid) || 0);
    var interestPaid = Math.max(0, Number(record.interestPaid) || 0);
    var expectedMonthlyInterest = Math.floor(runningBalance * (annualInterestRate / 100) / 12);
    var interestMonthsPaid = Math.max(0, parseInt(record.interestMonthsPaid, 10) || 0);

    if ((principalPaid + interestPaid) <= 0) {
      throw new Error('รายการรับชำระต้องมียอดชำระมากกว่า 0 | สัญญา ' + normalizedContract);
    }

    if (principalPaid > runningBalance) {
      throw new Error('ยอดชำระเงินต้นเกินยอดคงค้าง | สัญญา ' + normalizedContract);
    }

    if (isOffsetPayment) {
      if (principalPaid !== runningBalance) {
        throw new Error('การกลบหนี้ต้องชำระเต็มยอดคงค้างของสัญญา | สัญญา ' + normalizedContract);
      }
      if (interestPaid !== 0) {
        throw new Error('การกลบหนี้ต้องไม่มียอดดอกเบี้ยในรายการเดียวกัน | สัญญา ' + normalizedContract);
      }
      interestMonthsPaid = 0;
    } else {
      if (interestPaid > 0 && interestMonthsPaid <= 0) {
        if (expectedMonthlyInterest <= 0) {
          throw new Error('ไม่สามารถคำนวณจำนวนงวดดอกจากยอดดอกเบี้ยได้ | สัญญา ' + normalizedContract);
        }
        var inferredInstallments = interestPaid / expectedMonthlyInterest;
        if (Math.abs(inferredInstallments - Math.round(inferredInstallments)) > 0.0001) {
          throw new Error('ยอดดอกเบี้ยไม่สอดคล้องกับจำนวนงวดดอก | สัญญา ' + normalizedContract);
        }
        interestMonthsPaid = Math.max(1, Math.round(inferredInstallments));
      }

      if (interestPaid <= 0 && principalPaid > 0 && expectedMonthlyInterest > 0) {
        throw new Error('การชำระเงินต้นต้องมีดอกเบี้ยตามกติกาของระบบ | สัญญา ' + normalizedContract);
      }

      if (interestMonthsPaid > 0) {
        if (annualStateBefore.maxAllowedPaymentInstallments <= 0) {
          throw new Error('สัญญานี้ชำระดอกเบี้ยครบตามปีแล้ว ไม่สามารถบันทึกซ้ำได้ | สัญญา ' + normalizedContract);
        }
        if (interestMonthsPaid > annualStateBefore.maxAllowedPaymentInstallments) {
          throw new Error('จำนวนงวดดอกเกินกว่าที่ระบบอนุญาต (สูงสุด ' + annualStateBefore.maxAllowedPaymentInstallments + ' งวด) | สัญญา ' + normalizedContract);
        }
        if (expectedMonthlyInterest <= 0) {
          throw new Error('ไม่สามารถคำนวณดอกเบี้ยต่อเดือนได้ | สัญญา ' + normalizedContract);
        }
        if (interestPaid !== expectedMonthlyInterest * interestMonthsPaid) {
          throw new Error('ยอดชำระดอกเบี้ยต้องตรงกับจำนวนงวดดอกที่ชำระ | สัญญา ' + normalizedContract);
        }
      } else if (interestPaid > 0) {
        throw new Error('ยอดดอกเบี้ยไม่ถูกต้อง | สัญญา ' + normalizedContract);
      }
    }

    var newBalance = Math.max(0, runningBalance - principalPaid);
    var paidInstallmentsAfter = paidInstallmentsBefore + interestMonthsPaid;
    var annualStateAfter = isOffsetPayment
      ? annualStateBefore
      : calculateLoanAnnualInterestState_(loanRow[9], paidInstallmentsAfter, record.timestamp, newBalance, 'ปกติ');

    var missedBefore = Math.max(0, Number(annualStateBefore.missedInterestMonths) || 0);
    var missedAfter = isOffsetPayment || shouldExcludeLoanFromArrearsCount_(newBalance, 'ปกติ')
      ? 0
      : Math.max(0, Number(annualStateAfter.missedInterestMonths) || 0);

    var computedNote = normalizedNote;
    if (isOffsetPayment) {
      computedNote = 'กลบหนี้ด้วยเงินฝากสัจจะ';
    } else if (newBalance <= 0) {
      computedNote = 'ปิดบัญชี';
    } else {
      computedNote = sanitizeManagedPaymentNote_(computedNote);
    }

    record.contract = normalizedContract;
    record.memberId = authoritativeMemberId;
    record.memberName = authoritativeMemberName;
    record.computedInterestMonthsPaid = interestMonthsPaid;
    record.computedMissedBefore = missedBefore;
    record.computedMissedAfter = missedAfter;
    record.computedNewBalance = newBalance;
    record.computedNote = computedNote;
    record.computedStatus = deriveLoanStatusFromState_(newBalance, missedAfter, isOffsetPayment ? 'ปิดบัญชี(กลบหนี้)' : (newBalance <= 0 ? 'ปิดบัญชี' : 'ปกติ'));

    record.baseRowValues = [
      record.txId,
      record.timestamp,
      normalizedContract,
      authoritativeMemberId,
      principalPaid,
      interestPaid,
      newBalance,
      computedNote,
      record.actorName || getCurrentActorName_(),
      authoritativeMemberName,
      record.txStatus || 'ปกติ',
      interestMonthsPaid,
      missedBefore,
      missedAfter
    ];

    runningBalance = newBalance;
    runningStatus = record.computedStatus;
    paidInstallmentsByYear[txYearBe] = paidInstallmentsAfter;
  }

  return {
    contractNo: normalizedContract,
    loanRowIndex: loanRowIndex,
    loanRow: loanRow,
    records: records,
    finalLoanState: getLoanStatePreviewAsOfNow_(loanRow, records)
  };
}

function persistManagedContractRecalculationPlan_(ss, plan, deleteEntries) {
  if (!plan) return;

  var records = plan.records || [];
  var rowsToDeleteBySheet = {};
  var deleteList = deleteEntries || [];
  var appendRowsBySheet = {};
  var updateRowsBySheet = {};

  for (var i = 0; i < records.length; i++) {
    var record = records[i];
    var targetSheetName = determineManagedTargetSheetName_(record.sourceSheetName, record.timestamp);
    var writableValues = buildManagedWritableValuesForSheet_(targetSheetName, record.baseRowValues, record.archiveTail);

    if (targetSheetName === record.sourceSheetName && record.sourceRowIndex > 1) {
      if (!updateRowsBySheet[targetSheetName]) updateRowsBySheet[targetSheetName] = [];
      updateRowsBySheet[targetSheetName].push({
        rowIndex: record.sourceRowIndex,
        values: writableValues
      });
    } else {
      if (!appendRowsBySheet[targetSheetName]) appendRowsBySheet[targetSheetName] = [];
      appendRowsBySheet[targetSheetName].push(writableValues);

      if (record.sourceSheetName && record.sourceRowIndex > 1) {
        if (!rowsToDeleteBySheet[record.sourceSheetName]) rowsToDeleteBySheet[record.sourceSheetName] = {};
        rowsToDeleteBySheet[record.sourceSheetName][record.sourceRowIndex] = true;
      }
    }
  }

  var updateSheetNames = Object.keys(updateRowsBySheet);
  for (var u = 0; u < updateSheetNames.length; u++) {
    var updateSheetName = updateSheetNames[u];
    var updateSheet = getManagedTargetSheetForName_(ss, updateSheetName);
    writeSheetRowsBatchByIndexes_(updateSheet, updateRowsBySheet[updateSheetName]);
    invalidateSheetExecutionCaches_(updateSheet);
  }

  for (var targetSheetName in appendRowsBySheet) {
    if (!appendRowsBySheet.hasOwnProperty(targetSheetName)) continue;
    var targetSheet = getManagedTargetSheetForName_(ss, targetSheetName);
    var rowsToAppend = appendRowsBySheet[targetSheetName];
    if (rowsToAppend.length > 0) {
      appendRowsSafely_(targetSheet, rowsToAppend, 'เพิ่มข้อมูลปรับปรุงรายการรับชำระในชีต ' + targetSheet.getName());
    }
  }

  for (var d = 0; d < deleteList.length; d++) {
    var deleteEntry = deleteList[d];
    if (!deleteEntry || !deleteEntry.sheetName || !deleteEntry.rowIndex || deleteEntry.rowIndex <= 1) continue;
    if (!rowsToDeleteBySheet[deleteEntry.sheetName]) rowsToDeleteBySheet[deleteEntry.sheetName] = {};
    rowsToDeleteBySheet[deleteEntry.sheetName][deleteEntry.rowIndex] = true;
  }

  var deleteSheetNames = Object.keys(rowsToDeleteBySheet);
  for (var s = 0; s < deleteSheetNames.length; s++) {
    var sheetName = deleteSheetNames[s];
    var sheet = ss.getSheetByName(sheetName);
    if (!sheet) continue;

    var rowIndexes = Object.keys(rowsToDeleteBySheet[sheetName]).map(function(key) {
      return Number(key) || 0;
    }).filter(function(rowIndex) {
      return rowIndex > 1;
    });

    if (rowIndexes.length > 0) {
      deleteRowsByIndexes_(sheet, rowIndexes);
    }
  }

  var loanSheet = getSheetOrThrow(ss, 'Loans');
  var currentNextPayment = (plan.loanRow && plan.loanRow.length >= 8) ? plan.loanRow[7] : loanSheet.getRange(plan.loanRowIndex, 8).getValue();
  loanSheet.getRange(plan.loanRowIndex, 6, 1, 4).setValues([[
    plan.finalLoanState.balance,
    plan.finalLoanState.status,
    currentNextPayment,
    plan.finalLoanState.missedInterestMonths
  ]]);
  invalidateSheetExecutionCaches_(loanSheet);
}

function updateTodayPrincipalPaymentFromNormalFlow(payloadStr) {
  requirePermission_('payments.edit', 'ไม่มีสิทธิ์อัปเดตรายการรับชำระเดิม');
  var lock = LockService.getScriptLock();
  var lockAcquired = false;

  try {
    lock.waitLock(30000);
    lockAcquired = true;

    var payload = typeof payloadStr === 'string' ? JSON.parse(payloadStr) : (payloadStr || {});
    var txId = String(payload.id || '').trim();
    var requestedPrincipalPaid = Math.max(0, Number(payload.principalPaid) || 0);
    if (!txId) throw new Error('ไม่พบรหัสอ้างอิงของรายการรับชำระ');

    var ss = getDatabaseSpreadsheet_();
    var matchedTx = findTransactionInCurrentSheetsById_(ss, txId);
    if (!matchedTx) throw new Error('ไม่พบรายการรับชำระในชีต Transaction ที่ต้องการแก้ไข');
    if (isInactiveTransactionStatus_(String((matchedTx.values || [])[10] || '').trim())) {
      throw new Error('ไม่สามารถแก้ไขรายการที่ถูกยกเลิก/กลับรายการแล้ว');
    }

    var matchedTimestamp = normalizeThaiDateCellValue_((matchedTx.values || [])[1], true);
    var matchedDateOnly = String(matchedTimestamp || '').split(' ')[0] || '';
    var todayThaiDate = getThaiDate(new Date()).dateOnly;
    if (matchedDateOnly !== todayThaiDate) {
      throw new Error('อนุญาตให้อัปเดตรายการเดิมได้เฉพาะรายการของวันทำการปัจจุบันเท่านั้น');
    }

    var oldContract = String((matchedTx.values || [])[2] || '').trim();
    var originalMemberId = String((matchedTx.values || [])[3] || '').trim();
    var originalMemberName = String((matchedTx.values || [])[9] || '').trim();
    var originalPrincipalPaid = Math.max(0, Number((matchedTx.values || [])[4]) || 0);
    var originalInterestPaid = Math.max(0, Number((matchedTx.values || [])[5]) || 0);
    var originalNewBalance = Math.max(0, Number((matchedTx.values || [])[6]) || 0);
    var originalInterestMonthsPaid = Math.max(0, parseInt((matchedTx.values || [])[11], 10) || 0);
    var originalNote = String((matchedTx.values || [])[7] || '').trim();
    var broughtForwardBalance = originalPrincipalPaid + originalNewBalance;

    if (requestedPrincipalPaid > broughtForwardBalance) {
      throw new Error('ยอดชำระเงินต้นเกินยอดคงค้างก่อนรายการเดิม');
    }

    var normalizedPayload = normalizeManagedPaymentPayload_({
      id: txId,
      timestamp: matchedTimestamp,
      contract: oldContract,
      memberId: originalMemberId,
      memberName: originalMemberName,
      principalPaid: requestedPrincipalPaid,
      interestPaid: originalInterestPaid,
      interestMonthsPaid: originalInterestMonthsPaid,
      note: originalNote
    });

    var recalculationPlan = buildManagedContractRecalculationPlan_(ss, oldContract, {
      targetTxId: normalizedPayload.id,
      targetMode: 'replace',
      replacementMatch: matchedTx,
      replacementPayload: normalizedPayload
    });
    persistManagedContractRecalculationPlan_(ss, recalculationPlan);

    appendAuditLogSafe_(ss, {
      action: 'UPDATE_TODAY_PAYMENT_PRINCIPAL',
      entityType: 'TRANSACTION',
      entityId: normalizedPayload.id,
      referenceNo: normalizedPayload.id,
      before: buildAuditTransactionSnapshotFromValues_(matchedTx.values),
      after: {
        contract: oldContract,
        memberId: originalMemberId,
        memberName: originalMemberName,
        principalPaid: normalizedPayload.principalPaid,
        interestPaid: normalizedPayload.interestPaid,
        interestMonthsPaid: normalizedPayload.interestMonthsPaid,
        note: normalizedPayload.note,
        timestamp: normalizedPayload.timestamp
      },
      details: 'อัปเดตยอดชำระเงินต้นของรายการเดิมจากหน้ารับชำระเงินสำเร็จ'
    });
    logSystemActionFast(
      ss,
      'อัปเดตรายการรับชำระเดิมจากหน้ารับชำระเงิน',
      'Transaction ID: ' + normalizedPayload.id + ' | Contract: ' + oldContract + ' | Principal: ' + requestedPrincipalPaid + ' | Sheet: ' + String(matchedTx.sheet.getName() || '')
    );
    var refreshedTodayTx = findExistingTransactionById_(ss, normalizedPayload.id);
    syncTransactionIndexEntries_(ss, [{
      txId: normalizedPayload.id,
      rowValues: refreshedTodayTx ? refreshedTodayTx.values : null,
      rowIndex: refreshedTodayTx ? refreshedTodayTx.rowIndex : 0,
      sourceSheet: refreshedTodayTx ? refreshedTodayTx.sheet : null
    }]);
    syncPaymentSearchIndexForContracts_(ss, [oldContract]);
    syncPaymentTodayIndexEntries_(ss, [{
      txId: normalizedPayload.id,
      rowValues: refreshedTodayTx ? refreshedTodayTx.values : null,
      rowIndex: refreshedTodayTx ? refreshedTodayTx.rowIndex : 0
    }]);
    clearAppCache_();
    syncTransactionIndexVersionToCache_();
    syncPaymentSearchIndexVersionToCache_();
    syncPaymentTodayIndexVersionToCache_();

    return {
      status: 'Success',
      message: 'อัปเดตยอดชำระเงินต้นในรายการเดิมเรียบร้อยแล้ว',
      transactionId: normalizedPayload.id,
      timestamp: normalizedPayload.timestamp
    };
  } catch (e) {
    return {
      status: 'Error',
      message: 'อัปเดตยอดชำระเงินต้นในรายการเดิมไม่สำเร็จ: ' + e.toString()
    };
  } finally {
    if (lockAcquired) lock.releaseLock();
  }
}

function updateManagedPaymentRecord(payloadStr) {
  requirePermission_('payments.edit', 'ไม่มีสิทธิ์แก้ไขรายการรับชำระ');
  var lock = LockService.getScriptLock();
  var lockAcquired = false;

  try {
    lock.waitLock(30000);
    lockAcquired = true;

    var ss = getDatabaseSpreadsheet_();
    var payload = normalizeManagedPaymentPayload_(payloadStr);
    if (!payload.id) throw new Error('ไม่พบรหัสอ้างอิงของรายการรับชำระ');
    if (!payload.timestamp) throw new Error('กรุณาระบุวันเวลารายการ');

    var matchedTx = findExistingTransactionById_(ss, payload.id);
    if (!matchedTx) throw new Error('ไม่พบรายการรับชำระที่ต้องการแก้ไข');
    if (isInactiveTransactionStatus_(String((matchedTx.values || [])[10] || '').trim())) {
      throw new Error('ไม่สามารถแก้ไขรายการที่ถูกยกเลิก/กลับรายการแล้ว');
    }

    var oldContract = String((matchedTx.values || [])[2] || '').trim();
    var originalMemberId = String((matchedTx.values || [])[3] || '').trim();
    var originalMemberName = String((matchedTx.values || [])[9] || '').trim();
    var requestedContract = String(payload.contract || oldContract).trim();
    var requestedMemberId = String(payload.memberId || originalMemberId).trim();
    var requestedMemberName = String(payload.memberName || originalMemberName).trim();

    if (!requestedContract) throw new Error('กรุณาระบุเลขที่สัญญา');
    if (requestedContract !== oldContract) {
      throw new Error('ไม่อนุญาตให้ย้ายรายการรับชำระข้ามสัญญาด้วยการแก้ไขย้อนหลัง');
    }
    if (requestedMemberId && requestedMemberId !== originalMemberId) {
      throw new Error('ไม่อนุญาตให้เปลี่ยนรหัสสมาชิกของรายการรับชำระย้อนหลัง');
    }
    if (requestedMemberName && normalizeTextForMatch_(requestedMemberName) !== normalizeTextForMatch_(originalMemberName)) {
      throw new Error('ไม่อนุญาตให้เปลี่ยนชื่อสมาชิกของรายการรับชำระย้อนหลัง');
    }

    payload.contract = oldContract;
    payload.memberId = originalMemberId;
    payload.memberName = originalMemberName;

    var newContract = oldContract;
    var oldPlan = null;
    var newPlan = null;

    if (newContract === oldContract) {
      oldPlan = buildManagedContractRecalculationPlan_(ss, oldContract, {
        targetTxId: payload.id,
        targetMode: 'replace',
        replacementMatch: matchedTx,
        replacementPayload: payload
      });
    } else {
      oldPlan = buildManagedContractRecalculationPlan_(ss, oldContract, {
        targetTxId: payload.id,
        targetMode: 'remove',
        replacementMatch: matchedTx,
        replacementPayload: payload
      });
      newPlan = buildManagedContractRecalculationPlan_(ss, newContract, {
        targetTxId: payload.id,
        targetMode: 'inject',
        replacementMatch: matchedTx,
        replacementPayload: payload
      });
    }

    if (oldPlan) persistManagedContractRecalculationPlan_(ss, oldPlan);
    if (newPlan) persistManagedContractRecalculationPlan_(ss, newPlan);

    appendAuditLogSafe_(ss, {
      action: 'UPDATE_MANAGED_PAYMENT',
      entityType: 'TRANSACTION',
      entityId: payload.id,
      referenceNo: payload.id,
      before: buildAuditTransactionSnapshotFromValues_(matchedTx.values),
      after: {
        contract: oldContract,
        memberId: originalMemberId,
        memberName: originalMemberName,
        principalPaid: payload.principalPaid,
        interestPaid: payload.interestPaid,
        interestMonthsPaid: payload.interestMonthsPaid,
        note: payload.note,
        timestamp: payload.timestamp
      },
      details: 'แก้ไขรายการรับชำระย้อนหลังสำเร็จ'
    });
    logSystemActionFast(
      ss,
      'แก้ไขรายการรับชำระย้อนหลัง',
      'Transaction ID: ' + payload.id + ' | Contract: ' + oldContract + (newContract !== oldContract ? (' -> ' + newContract) : '') + ' | Sheet: ' + String(matchedTx.sheet.getName() || '')
    );
    var refreshedManagedTx = findExistingTransactionById_(ss, payload.id);
    syncTransactionIndexEntries_(ss, [{
      txId: payload.id,
      rowValues: refreshedManagedTx ? refreshedManagedTx.values : null,
      rowIndex: refreshedManagedTx ? refreshedManagedTx.rowIndex : 0,
      sourceSheet: refreshedManagedTx ? refreshedManagedTx.sheet : null
    }]);
    syncPaymentSearchIndexForContracts_(ss, [oldContract]);
    syncPaymentTodayIndexEntries_(ss, [{
      txId: payload.id,
      rowValues: refreshedManagedTx ? refreshedManagedTx.values : null,
      rowIndex: refreshedManagedTx ? refreshedManagedTx.rowIndex : 0
    }]);
    clearAppCache_();
    syncTransactionIndexVersionToCache_();
    syncPaymentSearchIndexVersionToCache_();
    syncPaymentTodayIndexVersionToCache_();

    return {
      status: 'Success',
      message: 'บันทึกการแก้ไขรายการรับชำระเรียบร้อยแล้ว'
    };
  } catch (e) {
    return {
      status: 'Error',
      message: 'บันทึกการแก้ไขรายการรับชำระไม่สำเร็จ: ' + e.toString()
    };
  } finally {
    if (lockAcquired) lock.releaseLock();
  }
}

function deleteManagedPaymentRecord(recordId, reason) {
  requirePermission_('payments.reverse', 'ไม่มีสิทธิ์ลบรายการรับชำระ');
  var lock = LockService.getScriptLock();
  var lockAcquired = false;

  try {
    lock.waitLock(30000);
    lockAcquired = true;

    var txId = String(recordId || '').trim();
    if (!txId) throw new Error('ไม่พบรหัสอ้างอิงของรายการรับชำระ');

    var ss = getDatabaseSpreadsheet_();
    var matchedTx = findExistingTransactionById_(ss, txId);
    if (!matchedTx) throw new Error('ไม่พบรายการรับชำระที่ต้องการลบ');
    if (isInactiveTransactionStatus_(String((matchedTx.values || [])[10] || '').trim())) {
      throw new Error('รายการนี้ถูกยกเลิก/กลับรายการแล้ว');
    }

    return removeManagedPaymentByMatchedTransaction_(
      ss,
      matchedTx,
      'ลบรายการรับชำระย้อนหลัง',
      'ลบรายการรับชำระเรียบร้อยแล้ว และคืนยอดสัญญาอัตโนมัติ'
    );
  } catch (e) {
    return {
      status: 'Error',
      message: 'ลบรายการรับชำระไม่สำเร็จ: ' + e.toString()
    };
  } finally {
    if (lockAcquired) lock.releaseLock();
  }
}

function getRegisterReportTransactions(memberId, reportDateInput) {
  requirePermission_('reports.view', 'ไม่มีสิทธิ์ดูรายงาน');
  try {
    var normalizedMemberId = normalizeTextForMatch_(memberId);
    if (!normalizedMemberId) {
      return {
        status: 'Error',
        message: 'ไม่พบรหัสสมาชิกสำหรับรายงานทะเบียนคุม'
      };
    }

    var reportDateParts = parseReportDateInput_(reportDateInput) || parseThaiDateParts_(new Date());
    var targetYearBe = Number(reportDateParts && reportDateParts.yearBe) || (new Date().getFullYear() + 543);
    var cacheKey = 'registerReportTransactions::' + normalizedMemberId + '::' + targetYearBe;
    var cached = getJsonCache_(cacheKey);
    if (cached && Array.isArray(cached.transactions)) {
      return {
        status: 'Success',
        memberId: normalizedMemberId,
        targetYearBe: targetYearBe,
        transactions: cached.transactions,
        indexed: true,
        cached: true
      };
    }

    var ss = ensureRuntimeDatabase_();
    var indexedRecords = getIndexedMemberTransactionRecords_(ss, normalizedMemberId);
    var transactions = [];
    var seenKeys = {};

    for (var i = 0; i < indexedRecords.length; i++) {
      var record = indexedRecords[i];
      if (Number(targetYearBe) && Number(record.yearBe) !== Number(targetYearBe)) continue;

      var dedupeKey = String(record.id || '').trim() || [
        record.timestamp,
        record.contract,
        record.memberId,
        record.principalPaid,
        record.interestPaid,
        record.newBalance,
        record.note
      ].join('|');

      if (seenKeys[dedupeKey]) continue;
      seenKeys[dedupeKey] = true;

      transactions.push({
        id: record.id,
        timestamp: record.timestamp,
        contract: record.contract,
        memberId: record.memberId,
        principalPaid: record.principalPaid,
        interestPaid: record.interestPaid,
        newBalance: record.newBalance,
        note: record.note,
        memberName: record.memberName,
        dateOnly: record.dateOnly,
        dateNumber: record.dateNumber
      });
    }

    transactions.sort(compareTransactionRecordOrderAsc_);

    var payload = {
      status: 'Success',
      memberId: normalizedMemberId,
      targetYearBe: targetYearBe,
      transactions: transactions,
      indexed: true,
      cached: false
    };
    putJsonCache_(cacheKey, { transactions: transactions }, 120);
    return payload;
  } catch (e) {
    return {
      status: 'Error',
      message: 'โหลดข้อมูลรายงานทะเบียนคุมไม่สำเร็จ: ' + e.toString()
    };
  }
}

function getPassbookTransactions(contractInput, memberId) {
  requirePermission_('payments.create', 'ไม่มีสิทธิ์ดูสมุดคู่ฝาก');
  try {
    var ss = ensureRuntimeDatabase_();
    var normalizedMemberId = normalizeTextForMatch_(memberId);
    var contractTokens = String(contractInput || '')
      .split(',')
      .map(function(item) { return String(item || '').trim(); })
      .filter(function(item) { return !!item; });
    var contractSet = {};
    for (var c = 0; c < contractTokens.length; c++) {
      contractSet[contractTokens[c]] = true;
    }

    var sourceRecords = [];
    if (contractTokens.length > 0) {
      var seenContractKeys = {};
      for (var ct = 0; ct < contractTokens.length; ct++) {
        var indexedContractRecords = getIndexedContractTransactionRecords_(ss, contractTokens[ct]);
        for (var ic = 0; ic < indexedContractRecords.length; ic++) {
          var contractRecord = indexedContractRecords[ic];
          var contractRecordKey = String(contractRecord.id || '').trim() || [
            contractRecord.timestamp,
            contractRecord.contract,
            contractRecord.memberId,
            contractRecord.principalPaid,
            contractRecord.interestPaid,
            contractRecord.newBalance,
            contractRecord.note,
            contractRecord.sourceSheet,
            String(contractRecord.rowIndex || 0)
          ].join('|');
          if (seenContractKeys[contractRecordKey]) continue;
          seenContractKeys[contractRecordKey] = true;
          sourceRecords.push(contractRecord);
        }
      }
    } else if (normalizedMemberId) {
      sourceRecords = getIndexedMemberTransactionRecords_(ss, normalizedMemberId);
    }

    var transactions = [];
    var seenKeys = {};

    for (var i = 0; i < sourceRecords.length; i++) {
      var record = sourceRecords[i];
      var contractNo = String(record.contract || '').trim();
      var rowMemberId = String(record.memberId || '').trim();

      if (contractTokens.length > 0 && !contractSet[contractNo]) continue;
      if (!contractNo && !rowMemberId) continue;
      if (contractTokens.length === 0 && normalizedMemberId && normalizeTextForMatch_(rowMemberId) !== normalizedMemberId) continue;

      var dedupeKey = String(record.id || '').trim() || [
        record.timestamp,
        contractNo,
        rowMemberId,
        record.principalPaid,
        record.interestPaid,
        record.newBalance,
        record.note
      ].join('|');
      if (seenKeys[dedupeKey]) continue;
      seenKeys[dedupeKey] = true;

      transactions.push({
        id: record.id,
        timestamp: record.timestamp,
        contract: contractNo,
        memberId: rowMemberId,
        principalPaid: record.principalPaid,
        interestPaid: record.interestPaid,
        newBalance: record.newBalance,
        note: record.note,
        memberName: record.memberName,
        dateOnly: record.dateOnly,
        dateNumber: record.dateNumber
      });
    }

    transactions.sort(compareTransactionRecordOrderAsc_);

    return {
      status: 'Success',
      contractInput: contractInput || '',
      memberId: normalizedMemberId || '',
      transactions: transactions,
      indexed: true
    };
  } catch (e) {
    return {
      status: 'Error',
      message: 'โหลดข้อมูลสมุดคู่ฝากไม่สำเร็จ: ' + e.toString()
    };
  }
}

function getLoanCreatedAtNumberForReport_(loanRow) {
  if (!loanRow) return 0;
  return getThaiDateNumber_(loanRow[9]);
}


function buildArchivedTransactionIdSetFromIndex_(ss) {
  var archivedSet = {};
  var spreadsheet = ss || getDatabaseSpreadsheet_();
  var indexSheet = ensureFreshTransactionIndexSheet_(spreadsheet);
  if (!indexSheet || indexSheet.getLastRow() <= 1) return archivedSet;

  var values = indexSheet.getRange(2, 1, indexSheet.getLastRow() - 1, Math.min(indexSheet.getLastColumn(), TRANSACTION_INDEX_HEADERS.length)).getValues();
  for (var i = 0; i < values.length; i++) {
    var txId = String(values[i][0] || '').trim();
    var sourceSheetName = String(values[i][10] || '').trim();
    if (!txId) continue;
    if (!parseTransactionsArchiveSheetMeta_(sourceSheetName)) continue;
    archivedSet[txId] = true;
  }
  return archivedSet;
}

function backupTransactionsToMonthlySheets() {
  requirePermission_('backup.run', 'ไม่มีสิทธิ์ Backup ธุรกรรม');

  var lock = LockService.getScriptLock();
  var lockAcquired = false;
  try {
    lock.waitLock(30000);
    lockAcquired = true;

    var ss = getDatabaseSpreadsheet_();
    var currentSheets = listCurrentTransactionSheets_(ss);
    var now = new Date();
    var nowInfo = getThaiDate(now);
    var rowsByTargetSheet = {};
    var targetSheetCountMap = {};
    var rowsToDeleteBySheet = {};
    var archivedTransactionIdSet = buildArchivedTransactionIdSetFromIndex_(ss);
    var archivedTransactions = 0;
    var alreadyArchivedTransactions = 0;
    var invalidDateRows = 0;
    var targetSheetNames = {};

    for (var s = 0; s < currentSheets.length; s++) {
      var sourceSheet = currentSheets[s];
      if (!sourceSheet) continue;
      ensureTransactionsSheetSchema_(sourceSheet);
      if (sourceSheet.getLastRow() <= 1) continue;

      var rowCount = sourceSheet.getLastRow() - 1;
      var sourceRows = sourceSheet.getRange(2, 1, rowCount, 14).getValues();
      var sourceSheetName = String(sourceSheet.getName() || 'Transactions');
      var deleteRows = [];

      for (var r = 0; r < sourceRows.length; r++) {
        var row = sourceRows[r];
        var txId = String(row[0] || '').trim();
        if (!txId) continue;

        var txDateParts = parseThaiDateParts_(row[1]);
        if (!txDateParts || !txDateParts.yearBe || !txDateParts.month) {
          invalidDateRows += 1;
          continue;
        }

        if (archivedTransactionIdSet[txId]) {
          alreadyArchivedTransactions += 1;
          deleteRows.push(r + 2);
          continue;
        }

        var targetYearBe = Number(txDateParts.yearBe) || 0;
        var targetMonth = Math.min(12, Math.max(1, Number(txDateParts.month) || 1));
        var targetSheetName = getTransactionsArchiveSheetName_(targetYearBe, targetMonth);
        var archiveRow = row.slice(0, 14);
        while (archiveRow.length < 14) archiveRow.push('');
        archiveRow = archiveRow.concat([
          nowInfo.dateTime,
          targetYearBe + '-' + padNumber_(targetMonth, 2)
        ]);

        if (!rowsByTargetSheet[targetSheetName]) rowsByTargetSheet[targetSheetName] = [];
        rowsByTargetSheet[targetSheetName].push(archiveRow);
        targetSheetCountMap[targetSheetName] = (targetSheetCountMap[targetSheetName] || 0) + 1;
        targetSheetNames[targetSheetName] = true;
        archivedTransactionIdSet[txId] = true;
        archivedTransactions += 1;
        deleteRows.push(r + 2);
      }

      if (deleteRows.length > 0) {
        rowsToDeleteBySheet[sourceSheetName] = deleteRows;
      }
    }

    var targetSheetNamesList = Object.keys(targetSheetNames);
    if (targetSheetNamesList.length > 0) {
      assertSheetAppendPlanCapacity_(ss, targetSheetCountMap, 16, 'Backup ธุรกรรมไปชีตรายเดือน');
      for (var t = 0; t < targetSheetNamesList.length; t++) {
        var monthlySheetName = targetSheetNamesList[t];
        var meta = parseTransactionsArchiveSheetMeta_(monthlySheetName);
        if (!meta || !meta.yearBe || !meta.month) continue;
        var archiveSheet = ensureTransactionsArchiveSheet_(ss, meta.yearBe, meta.month);
        appendRowsSafely_(archiveSheet, rowsByTargetSheet[monthlySheetName], 'Backup ธุรกรรมไปชีต ' + monthlySheetName);
      }
    }

    var deleteSheetNames = Object.keys(rowsToDeleteBySheet);
    for (var d = 0; d < deleteSheetNames.length; d++) {
      var deleteSheetName = deleteSheetNames[d];
      var deleteSheet = ss.getSheetByName(deleteSheetName);
      if (!deleteSheet) continue;
      deleteRowsByIndexes_(deleteSheet, rowsToDeleteBySheet[deleteSheetName]);
    }

    rebuildTransactionIndexSheet_(ss);
    clearAppCache_();

    var summarySheetLabel = targetSheetNamesList.length === 1 ? targetSheetNamesList[0] : (targetSheetNamesList.length > 1 ? targetSheetNamesList.join(', ') : 'ไม่มีรายการให้ย้าย');
    appendAuditLogSafe_(ss, {
      action: 'BACKUP_TRANSACTIONS_TO_MONTHLY',
      entityType: 'SYSTEM',
      entityId: 'Transactions',
      referenceNo: summarySheetLabel,
      after: { archivedTransactions: archivedTransactions, alreadyArchivedTransactions: alreadyArchivedTransactions, invalidDateRows: invalidDateRows, archiveSheetNames: targetSheetNamesList },
      details: 'Backup ธุรกรรมรายเดือนสำเร็จ'
    });
    logSystemActionFast(
      ss,
      'Backup ธุรกรรมรายเดือน',
      'ย้ายธุรกรรม ' + archivedTransactions + ' รายการ | ชีตปลายทาง: ' + summarySheetLabel +
      (alreadyArchivedTransactions > 0 ? ' | ข้ามรายการซ้ำ ' + alreadyArchivedTransactions + ' รายการ' : '') +
      (invalidDateRows > 0 ? ' | วันที่ธุรกรรมไม่สมบูรณ์ ' + invalidDateRows + ' รายการ' : '')
    );

    return {
      status: 'Success',
      archivedTransactions: archivedTransactions,
      alreadyArchivedTransactions: alreadyArchivedTransactions,
      invalidDateRows: invalidDateRows,
      archiveSheetName: targetSheetNamesList.length === 1 ? targetSheetNamesList[0] : '',
      archiveSheetNames: targetSheetNamesList,
      message: archivedTransactions > 0 ? 'Backup ธุรกรรมสำเร็จ' : 'ไม่พบรายการธุรกรรมที่ต้อง Backup'
    };
  } catch (e) {
    console.error('backupTransactionsToMonthlySheets failed', e);
    return { status: 'Error', message: 'Backup ธุรกรรมไม่สำเร็จ: ' + e.toString() };
  } finally {
    if (lockAcquired) lock.releaseLock();
  }
}

function closeAccountingPeriod() {
  requirePermission_('accounting.close', 'ไม่มีสิทธิ์ปิดยอดบัญชี');

  var lock = LockService.getScriptLock();
  var lockAcquired = false;
  try {
    lock.waitLock(30000);
    lockAcquired = true;

    var ss = getDatabaseSpreadsheet_();
    var now = new Date();
    var settingsSheet = ss.getSheetByName('Settings') || createSheetSafely_(ss, 'Settings', 2, 2, 'เตรียมชีต Settings');
    var closeContext = getCloseAccountingReferenceContext_(settingsSheet, now);
    var closeMonthKey = String((closeContext && closeContext.monthKey) || '').trim();
    if (!closeMonthKey) {
      throw new Error('ไม่สามารถระบุรอบปิดยอดบัญชีได้');
    }
    if (!closeContext.hasOperatingDay || !closeContext.operatingDate) {
      throw new Error('ยังไม่ได้กำหนดวันทำการของรอบ ' + closeMonthKey + ' กรุณาตั้งค่าที่หน้าตั้งค่าระบบก่อน');
    }
    if (!closeContext.hasReachedOperatingDay) {
      throw new Error('ยังไม่ถึงวันทำการของรอบ ' + closeMonthKey + ' (' + (closeContext.operatingDateIso || '-') + ') จึงยังปิดยอดบัญชีไม่ได้');
    }
    var lastClosedMonthKey = String(getSettingValue_(settingsSheet, 'LastClosedAccountingMonth') || '').trim();
    if (lastClosedMonthKey === closeMonthKey) {
      throw new Error('รอบ ' + closeMonthKey + ' ถูกปิดยอดบัญชีไปแล้ว');
    }
    var result = recalculateAnnualInterestArrearsSnapshots_(ss, closeContext.operatingDate, { includeCurrentMonthInstallment: true });
    upsertSettingValue_(settingsSheet, 'LastClosedAccountingMonth', closeMonthKey);
    upsertSettingValue_(settingsSheet, 'LastClosedAccountingAt', getThaiDate(now).dateTime);
    upsertSettingValue_(settingsSheet, 'LastClosedAccountingReferenceDate', closeContext.operatingDateIso || '');

    appendAuditLogSafe_(ss, {
      action: 'CLOSE_ACCOUNTING_PERIOD',
      entityType: 'SYSTEM',
      entityId: closeMonthKey,
      referenceNo: closeMonthKey,
      after: { processedLoans: result.processedLoans || 0, changedLoans: result.changedLoans || 0, totalArrearsLoans: result.totalArrearsLoans || 0, totalPaidInstallmentsYtd: result.totalPaidInstallmentsYtd || 0, referenceOperatingDate: closeContext.operatingDateIso || '' },
      details: 'ปิดยอดบัญชีสำเร็จ'
    });
    logSystemActionFast(
      ss,
      'ปิดยอดบัญชี',
      'คำนวณค้างดอกอัตโนมัติ | เดือน ' + closeMonthKey +
      ' | วันทำการ ' + (closeContext.operatingDateIso || '-') +
      ' | ตรวจ ' + (result.processedLoans || 0) + ' สัญญา' +
      ' | เปลี่ยนแปลง ' + (result.changedLoans || 0) + ' สัญญา' +
      ' | ค้างดอก ' + (result.totalArrearsLoans || 0) + ' สัญญา'
    );

    clearAppCache_();
    return {
      status: 'Success',
      closedMonthKey: closeMonthKey,
      processedLoans: result.processedLoans || 0,
      changedLoans: result.changedLoans || 0,
      totalArrearsLoans: result.totalArrearsLoans || 0,
      totalPaidInstallmentsYtd: result.totalPaidInstallmentsYtd || 0,
      targetYearBe: result.targetYearBe || ((parseThaiDateParts_(closeContext.operatingDate || now) || {}).yearBe || (now.getFullYear() + 543)),
      referenceOperatingDate: closeContext.operatingDateIso || '',
      message: 'ปิดยอดบัญชีสำเร็จ'
    };
  } catch (e) {
    console.error('closeAccountingPeriod failed', e);
    return { status: 'Error', message: 'ปิดยอดบัญชีไม่สำเร็จ: ' + e.toString() };
  } finally {
    if (lockAcquired) lock.releaseLock();
  }
}

function closeDayEnd() {
  return closeAccountingPeriod();
}

function getReportArchiveIndexHeaders_() {
  return [
    'ArchiveKey', 'DocumentNo', 'ReportType', 'ReportTypeLabel', 'YearBE', 'Month', 'Day',
    'PeriodLabel', 'ReportDateDisplay', 'MemberId', 'MemberName', 'ReportTitle',
    'FileName', 'DriveFileId', 'DriveUrl', 'FolderPath', 'CreatedAt', 'CreatedBy', 'Status',
    'SnapshotKey', 'SnapshotSummary', 'AsOfLabel',
    'RenderSnapshotKey', 'RenderSnapshotSummary', 'RenderAsOfLabel',
    'ValidationStatus', 'ValidationMessage', 'ValidatedAt',
    'ValidationScope', 'RenderMetricCount', 'RenderMetricKeys'
  ];
}

function ensureReportArchiveIndexSheet_(ss) {
  var spreadsheet = ss || getDatabaseSpreadsheet_();
  var headers = getReportArchiveIndexHeaders_();
  var sheet = spreadsheet.getSheetByName('ReportArchives') || createSheetSafely_(spreadsheet, 'ReportArchives', 2, headers.length, 'เตรียมชีตดัชนีรายงาน PDF');
  ensureSheetGridSizeSafely_(sheet, Math.max(sheet.getMaxRows(), 2), headers.length, 'เตรียมชีตดัชนีรายงาน PDF');
  sheet.getRange(1, 1, 1, headers.length).setValues([headers]).setFontWeight('bold').setBackground('#ede9fe');
  if (sheet.getFrozenRows() < 1) sheet.setFrozenRows(1);
  setPlainTextColumnFormat_(sheet, [1, 2, 3, 4, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31]);
  return sheet;
}

function sanitizeDriveName_(value) {
  return String(value || '')
    .replace(/[\\\/:*?"<>|#\[\]\n\r]+/g, ' ')
    .replace(/\s+/g, ' ')
    .trim();
}

function getThaiMonthLabelByNumber_(monthNumber) {
  var labels = ['มกราคม', 'กุมภาพันธ์', 'มีนาคม', 'เมษายน', 'พฤษภาคม', 'มิถุนายน', 'กรกฎาคม', 'สิงหาคม', 'กันยายน', 'ตุลาคม', 'พฤศจิกายน', 'ธันวาคม'];
  var index = Math.max(1, Math.min(12, Number(monthNumber) || 1)) - 1;
  return labels[index];
}

function buildThaiDateTimeSortKey_(value) {
  var raw = String(value || '').trim();
  var match = raw.match(/^(\d{2})\/(\d{2})\/(\d{4})(?:\s+(\d{2}):(\d{2})(?::(\d{2}))?)?$/);
  if (!match) return 0;
  var day = Number(match[1]) || 0;
  var month = Number(match[2]) || 0;
  var yearBe = Number(match[3]) || 0;
  var yearAd = yearBe >= 2400 ? (yearBe - 543) : yearBe;
  var hour = Number(match[4] || 0) || 0;
  var minute = Number(match[5] || 0) || 0;
  var second = Number(match[6] || 0) || 0;
  return Number(
    String(yearAd).padStart(4, '0') +
    padNumber_(month, 2) +
    padNumber_(day, 2) +
    padNumber_(hour, 2) +
    padNumber_(minute, 2) +
    padNumber_(second, 2)
  ) || 0;
}

function getOrCreateChildFolderByName_(parentFolder, folderName) {
  var safeName = sanitizeDriveName_(folderName) || 'Reports';
  var iterator = parentFolder.getFoldersByName(safeName);
  if (iterator.hasNext()) return iterator.next();
  return parentFolder.createFolder(safeName);
}

function getReportArchiveRootFolder_(ss) {
  var spreadsheet = ss || getDatabaseSpreadsheet_();
  var settingsSheet = spreadsheet.getSheetByName('Settings') || createSheetSafely_(spreadsheet, 'Settings', 2, 2, 'เตรียมชีต Settings');
  var existingFolderId = String(getSettingValue_(settingsSheet, 'ReportArchiveRootFolderId') || '').trim();
  if (existingFolderId) {
    try {
      return DriveApp.getFolderById(existingFolderId);
    } catch (folderError) {}
  }

  var spreadsheetFile = DriveApp.getFileById(spreadsheet.getId());
  var parentIterator = spreadsheetFile.getParents();
  var parentFolder = parentIterator.hasNext() ? parentIterator.next() : DriveApp.getRootFolder();
  var folderName = sanitizeDriveName_(spreadsheet.getName() + ' - Report PDF Archive') || 'Report PDF Archive';
  var rootFolder = getOrCreateChildFolderByName_(parentFolder, folderName);
  upsertSettingValue_(settingsSheet, 'ReportArchiveRootFolderId', rootFolder.getId());
  upsertSettingValue_(settingsSheet, 'ReportArchiveRootFolderName', rootFolder.getName());
  return rootFolder;
}

function normalizeReportArchivePayload_(payload) {
  var safe = payload || {};
  var reportType = String(safe.reportType || 'daily').trim() || 'daily';
  var reportTypeLabel = reportType === 'register' ? 'รายงานทะเบียนคุม' : reportType === 'overdue_interest' ? 'รายงานหนี้คงค้าง' : 'รายงานส่งบัญชี';
  var reportDateParts = parseReportDateInput_(safe.reportDate) || parseThaiDateParts_(new Date()) || { day: 1, month: 1, yearBe: new Date().getFullYear() + 543, dateOnly: getThaiDate(new Date()).date };
  var yearBe = Number(safe.reportYearBe) || Number(reportDateParts.yearBe) || (new Date().getFullYear() + 543);
  var month = Number(safe.reportMonth) || Number(reportDateParts.month) || (new Date().getMonth() + 1);
  var day = Number(safe.reportDay) || Number(reportDateParts.day) || 1;
  var reportDateDisplay = String(safe.reportDateDisplay || reportDateParts.dateOnly || '').trim();
  if (!reportDateDisplay) reportDateDisplay = padNumber_(day, 2) + '/' + padNumber_(month, 2) + '/' + yearBe;
  var memberId = String(safe.memberId || '').trim();
  var memberName = String(safe.memberName || '').trim();
  var periodLabel = String(safe.periodLabel || '').trim();
  if (!periodLabel) {
    periodLabel = reportType === 'overdue_interest'
      ? (getThaiMonthLabelByNumber_(month) + ' ' + yearBe)
      : reportDateDisplay;
  }

  var archiveKey = reportType + '|' + yearBe + '-' + padNumber_(month, 2);
  if (reportType === 'daily') {
    archiveKey = 'daily|' + reportDateDisplay;
  } else if (reportType === 'register') {
    archiveKey = 'register|' + memberId + '|' + reportDateDisplay;
  } else if (reportType === 'overdue_interest') {
    archiveKey = 'overdue_interest|' + yearBe + '-' + padNumber_(month, 2);
  }

  var baseTitle = String(safe.title || '').trim();
  if (!baseTitle) {
    baseTitle = reportTypeLabel + ' ' + periodLabel;
    if (reportType === 'register' && memberId) {
      baseTitle += ' ' + memberId + (memberName ? (' ' + memberName) : '');
    }
  }
  if (reportType === 'overdue_interest' && reportDateDisplay) {
    var asOfSuffix = '(ข้อมูล ณ ' + reportDateDisplay + ')';
    if (baseTitle.indexOf(asOfSuffix) === -1) {
      baseTitle += ' ' + asOfSuffix;
    }
  }

  var fileName = sanitizeDriveName_(baseTitle) || ('report-' + archiveKey);
  if (!/\.pdf$/i.test(fileName)) fileName += '.pdf';
  var asOfLabel = String(safe.asOfLabel || safe.renderAsOfLabel || '').trim();
  if (!asOfLabel && reportDateDisplay) asOfLabel = 'ข้อมูล ณ วันที่ ' + reportDateDisplay;

  return {
    reportType: reportType,
    reportTypeLabel: reportTypeLabel,
    yearBe: yearBe,
    month: month,
    day: day,
    periodLabel: periodLabel,
    reportDateDisplay: reportDateDisplay,
    memberId: memberId,
    memberName: memberName,
    title: baseTitle,
    fileName: fileName,
    archiveKey: archiveKey,
    asOfLabel: asOfLabel
  };
}

function normalizeReportSnapshotNumber_(value) {
  var num = Number(value);
  if (!isFinite(num)) return 0;
  return Math.round(num * 100) / 100;
}

function buildDailyReportDeepMetrics_(snapshot) {
  var details = Array.isArray(snapshot && snapshot.details) ? snapshot.details : [];
  var rowsPerPage = 34;
  var totals = {
    detailCount: details.length,
    detailBfwBalance: 0,
    detailPrincipalPaid: 0,
    detailOffsetPaid: 0,
    detailInterestPaid: 0,
    detailCfwBalance: 0,
    detailTotalPaid: 0,
    overdueDetailCount: 0,
    closedTodayCount: 0,
    offsetOnlyCount: 0,
    mixedCaseContractCount: 0,
    fullPageCount: 0,
    lastPageRowCount: 0,
    pageCount: 1,
    pageRowHash: '1:0',
    renderedRowCount: rowsPerPage,
    renderedPageRowHash: '1:' + rowsPerPage,
    pageFooterHash: '1:0:0:0:0:0:0',
    renderedPageFooterHash: '1:0:0:0:0:0:0',
    pageBreakdownHash: '1:0:0:0:0:0:0:0',
    pageFlagHash: '1:0:0:0:0:0'
  };
  if (!details.length) return totals;
  var pageParts = [];
  var pageFlagParts = [];
  for (var i = 0; i < details.length; i++) {
    var row = details[i] || {};
    var principalPaid = Number(row.principalPaid) || 0;
    var offsetPaid = Number(row.offsetPaid) || 0;
    var interestPaid = Number(row.interestPaid) || 0;
    totals.detailBfwBalance += Number(row.broughtForward) || 0;
    totals.detailPrincipalPaid += principalPaid;
    totals.detailOffsetPaid += offsetPaid;
    totals.detailInterestPaid += interestPaid;
    totals.detailCfwBalance += Number(row.balance) || 0;
    if (row.isOverdue) totals.overdueDetailCount += 1;
    if (String(row.note || '').trim() === 'ปิดบัญชีวันนี้') totals.closedTodayCount += 1;
    if (offsetPaid > 0 && principalPaid === 0) totals.offsetOnlyCount += 1;
    if (String(row.mixedCaseDetail || '').trim()) totals.mixedCaseContractCount += 1;
  }
  totals.detailBfwBalance = normalizeReportSnapshotNumber_(totals.detailBfwBalance);
  totals.detailPrincipalPaid = normalizeReportSnapshotNumber_(totals.detailPrincipalPaid);
  totals.detailOffsetPaid = normalizeReportSnapshotNumber_(totals.detailOffsetPaid);
  totals.detailInterestPaid = normalizeReportSnapshotNumber_(totals.detailInterestPaid);
  totals.detailCfwBalance = normalizeReportSnapshotNumber_(totals.detailCfwBalance);
  totals.detailTotalPaid = normalizeReportSnapshotNumber_(totals.detailPrincipalPaid + totals.detailInterestPaid);
  for (var start = 0, pageNumber = 1; start < details.length; start += rowsPerPage, pageNumber++) {
    var pageRows = details.slice(start, start + rowsPerPage);
    var pageBfw = 0, pagePrincipal = 0, pageOffset = 0, pageInterest = 0, pageCfw = 0;
    var overdueCount = 0, closedTodayCount = 0, offsetOnlyCount = 0, mixedCaseCount = 0;
    for (var r = 0; r < pageRows.length; r++) {
      var item = pageRows[r] || {};
      var itemPrincipal = Number(item.principalPaid) || 0;
      var itemOffset = Number(item.offsetPaid) || 0;
      pageBfw += Number(item.broughtForward) || 0;
      pagePrincipal += itemPrincipal;
      pageOffset += itemOffset;
      pageInterest += Number(item.interestPaid) || 0;
      pageCfw += Number(item.balance) || 0;
      if (item.isOverdue) overdueCount += 1;
      if (String(item.note || '').trim() === 'ปิดบัญชีวันนี้') closedTodayCount += 1;
      if (itemOffset > 0 && itemPrincipal === 0) offsetOnlyCount += 1;
      if (String(item.mixedCaseDetail || '').trim()) mixedCaseCount += 1;
    }
    pageBfw = normalizeReportSnapshotNumber_(pageBfw);
    pagePrincipal = normalizeReportSnapshotNumber_(pagePrincipal);
    pageOffset = normalizeReportSnapshotNumber_(pageOffset);
    pageInterest = normalizeReportSnapshotNumber_(pageInterest);
    pageCfw = normalizeReportSnapshotNumber_(pageCfw);
    var pageTotalPaid = normalizeReportSnapshotNumber_(pagePrincipal + pageInterest);
    pageParts.push([pageNumber, pageRows.length, pageBfw, pagePrincipal, pageOffset, pageInterest, pageCfw, pageTotalPaid].join(':'));
    pageFlagParts.push([pageNumber, pageRows.length, overdueCount, closedTodayCount, offsetOnlyCount, mixedCaseCount].join(':'));
    totals.lastPageRowCount = pageRows.length;
  }
  totals.pageCount = Math.max(1, Math.ceil(details.length / rowsPerPage));
  totals.fullPageCount = pageParts.filter(function(part) {
    return Number(String(part || '').split(':')[1] || 0) === rowsPerPage;
  }).length;
  totals.pageRowHash = pageParts.map(function(part) {
    var cols = String(part || '').split(':');
    return [cols[0] || '1', cols[1] || '0'].join(':');
  }).join(';');
  totals.renderedRowCount = totals.pageCount * rowsPerPage;
  totals.renderedPageRowHash = [];
  for (var pageIdx = 1; pageIdx <= totals.pageCount; pageIdx++) {
    totals.renderedPageRowHash.push([pageIdx, rowsPerPage].join(':'));
  }
  totals.renderedPageRowHash = totals.renderedPageRowHash.join(';');
  totals.pageBreakdownHash = pageParts.join(';');
  totals.pageFooterHash = pageParts.map(function(part) {
    var cols = String(part || '').split(':');
    return [cols[0] || '1', cols[2] || '0', cols[3] || '0', cols[4] || '0', cols[5] || '0', cols[6] || '0', cols[7] || '0'].join(':');
  }).join(';');
  totals.renderedPageFooterHash = totals.pageFooterHash;
  totals.pageFlagHash = pageFlagParts.join(';');
  return totals;
}

function buildReportSnapshotKeyAndSummary_(meta, payload, snapshot) {
  var reportType = String((meta && meta.reportType) || 'daily').trim() || 'daily';
  var reportDateDisplay = String((meta && meta.reportDateDisplay) || '').trim();
  var asOfLabel = String(
    (snapshot && snapshot.asOfLabel) ||
    (payload && payload.asOfLabel) ||
    (meta && meta.asOfLabel) ||
    (reportDateDisplay ? ('ข้อมูล ณ วันที่ ' + reportDateDisplay) : '')
  ).trim();
  var snapshotKey = '';
  var snapshotSummary = '';

  if (reportType === 'daily') {
    var dailySummary = (snapshot && snapshot.summary) || {};
    var dailyDeepMetrics = buildDailyReportDeepMetrics_(snapshot);
    var dailyCount = dailyDeepMetrics.detailCount;
    var dailyTotalPaid = normalizeReportSnapshotNumber_(dailySummary.totalPaid);
    var dailyCfwBalance = normalizeReportSnapshotNumber_(dailySummary.cfwBalance);
    snapshotKey = [
      'daily',
      reportDateDisplay,
      dailyCount,
      normalizeReportSnapshotNumber_(dailySummary.bfwBalance),
      normalizeReportSnapshotNumber_(dailySummary.principalPaid),
      normalizeReportSnapshotNumber_(dailySummary.offsetPaid),
      normalizeReportSnapshotNumber_(dailySummary.interestPaid),
      dailyTotalPaid,
      dailyCfwBalance,
      dailyDeepMetrics.detailBfwBalance,
      dailyDeepMetrics.detailPrincipalPaid,
      dailyDeepMetrics.detailOffsetPaid,
      dailyDeepMetrics.detailInterestPaid,
      dailyDeepMetrics.detailCfwBalance,
      dailyDeepMetrics.pageBreakdownHash,
      dailyDeepMetrics.overdueDetailCount,
      dailyDeepMetrics.closedTodayCount,
      dailyDeepMetrics.offsetOnlyCount,
      dailyDeepMetrics.mixedCaseContractCount,
      dailyDeepMetrics.fullPageCount,
      dailyDeepMetrics.lastPageRowCount,
      dailyDeepMetrics.pageRowHash,
      dailyDeepMetrics.renderedRowCount,
      dailyDeepMetrics.renderedPageRowHash,
      dailyDeepMetrics.pageFooterHash,
      dailyDeepMetrics.renderedPageFooterHash,
      dailyDeepMetrics.pageFlagHash
    ].join('|');
    snapshotSummary = 'สัญญา ' + dailyCount + ' | ส่งบัญชี ' + dailyTotalPaid + ' | หนี้ยกไป ' + dailyCfwBalance + ' | รวมท้ายหน้า ' + dailyDeepMetrics.pageCount + ' หน้า | หน้าเต็ม 34 แถว ' + dailyDeepMetrics.fullPageCount + ' | พิมพ์จริง ' + dailyDeepMetrics.renderedRowCount + ' แถว';
    if (!asOfLabel && reportDateDisplay) asOfLabel = 'ข้อมูล ณ วันที่ ' + reportDateDisplay;
  } else if (reportType === 'register') {
    var member = (snapshot && snapshot.member) || {};
    var loanRecords = Array.isArray(snapshot && snapshot.loanRecords) ? snapshot.loanRecords : [];
    var totalBalance = 0;
    var overdueLoanCount = 0;
    for (var i = 0; i < loanRecords.length; i++) {
      var currentLoan = loanRecords[i] && loanRecords[i].loan ? loanRecords[i].loan : {};
      totalBalance += normalizeReportSnapshotNumber_(currentLoan.balance);
      if (Number(currentLoan.currentMissedInterestMonths || currentLoan.missedInterestMonths || 0) > 0) {
        overdueLoanCount += 1;
      }
    }
    totalBalance = normalizeReportSnapshotNumber_(totalBalance);
    var snapshotMemberId = String(member.id || (meta && meta.memberId) || '').trim();
    snapshotKey = ['register', reportDateDisplay, snapshotMemberId, loanRecords.length, totalBalance, overdueLoanCount].join('|');
    snapshotSummary = 'สมาชิก ' + (snapshotMemberId || '-') + ' | สัญญา ' + loanRecords.length + ' | ค้างดอก ' + overdueLoanCount + ' | หนี้รวม ' + totalBalance;
    if (!asOfLabel && reportDateDisplay) asOfLabel = 'ข้อมูล ณ วันที่ ' + reportDateDisplay;
  } else {
    var overdueSummary = (snapshot && snapshot.summary) || {};
    var overdueYearBe = Number((snapshot && snapshot.yearBe) || (meta && meta.yearBe) || 0) || 0;
    var overdueMonth = Number((snapshot && snapshot.month) || (meta && meta.month) || 0) || 0;
    var overdueTotalLoans = Number(overdueSummary.totalLoans || 0) || 0;
    var overdueLoans = Number(overdueSummary.overdueLoans || 0) || 0;
    var overdueTotalBalance = normalizeReportSnapshotNumber_(overdueSummary.totalBalance);
    snapshotKey = ['overdue_interest', overdueYearBe, overdueMonth, reportDateDisplay, overdueTotalLoans, overdueLoans, overdueTotalBalance].join('|');
    snapshotSummary = 'สัญญาคงค้าง ' + overdueTotalLoans + ' | ค้างดอก ' + overdueLoans + ' | หนี้รวม ' + overdueTotalBalance;
    if (!asOfLabel && reportDateDisplay) asOfLabel = 'ข้อมูล ณ วันที่ ' + reportDateDisplay;
  }

  return {
    snapshotKey: snapshotKey,
    snapshotSummary: snapshotSummary,
    asOfLabel: asOfLabel
  };
}

function getRequiredRenderMetricKeysByReportType_(reportType) {
  var normalizedType = String(reportType || 'daily').trim() || 'daily';
  if (normalizedType === 'daily') {
    return ['detailCount', 'bfwBalance', 'principalPaid', 'offsetPaid', 'interestPaid', 'totalPaid', 'cfwBalance', 'pageCount', 'mixedCaseNoteCount', 'detailBfwBalance', 'detailPrincipalPaid', 'detailOffsetPaid', 'detailInterestPaid', 'detailCfwBalance', 'pageBreakdownHash', 'overdueDetailCount', 'closedTodayCount', 'offsetOnlyCount', 'mixedCaseContractCount', 'fullPageCount', 'lastPageRowCount', 'pageRowHash', 'renderedRowCount', 'renderedPageRowHash', 'pageFooterHash', 'renderedPageFooterHash', 'pageFlagHash'];
  }
  if (normalizedType === 'register') {
    return ['memberId', 'loanCount', 'overdueLoanCount', 'totalBalance', 'historyRowCount'];
  }
  return ['periodLabel', 'totalLoans', 'overdueLoans', 'totalBalance', 'renderedDetailCount'];
}

function validateRenderSnapshotPayload_(payload, snapshotMeta, reportType) {
  var renderSnapshotKey = String((payload && payload.renderSnapshotKey) || '').trim();
  var renderSnapshotSummary = String((payload && payload.renderSnapshotSummary) || '').trim();
  var renderAsOfLabel = String((payload && payload.renderAsOfLabel) || '').trim();
  var renderValidationScope = String((payload && payload.renderValidationScope) || '').trim();
  var renderMetricCount = Number((payload && payload.renderMetricCount) || 0) || 0;
  var renderMetricKeys = String((payload && payload.renderMetricKeys) || '').trim();
  if (renderSnapshotKey && snapshotMeta && snapshotMeta.snapshotKey && renderSnapshotKey !== snapshotMeta.snapshotKey) {
    throw new Error('ภาพรายงานบนหน้าจอไม่ตรงกับ snapshot ที่กำลังจะบันทึก กรุณาเปิดรายงานใหม่แล้วลองอีกครั้ง');
  }
  if (renderAsOfLabel && snapshotMeta && snapshotMeta.asOfLabel && renderAsOfLabel !== snapshotMeta.asOfLabel) {
    throw new Error('วันที่อ้างอิงของภาพรายงานบนหน้าจอไม่ตรงกับ snapshot ที่กำลังจะบันทึก');
  }

  var validationStatus = 'Generated';
  var validationMessage = 'สร้างไฟล์จาก snapshot โดยตรง ไม่มีชั้น DOM ให้ตรวจ';
  if (renderSnapshotKey || renderSnapshotSummary || renderAsOfLabel) {
    var providedMetricKeys = renderMetricKeys ? renderMetricKeys.split(',').map(function(item) {
      return String(item || '').trim();
    }).filter(function(item) { return !!item; }) : [];
    var requiredMetricKeys = getRequiredRenderMetricKeysByReportType_(reportType);
    var missingMetricKeys = requiredMetricKeys.filter(function(key) {
      return providedMetricKeys.indexOf(String(key || '').trim()) === -1;
    });
    if (renderMetricCount > 0 && missingMetricKeys.length === 0) {
      validationStatus = 'Verified';
      validationMessage = 'ผ่านการตรวจ consistency ระหว่าง DOM พิมพ์กับ snapshot';
      if (renderValidationScope) {
        validationMessage += ' | ขอบเขต: ' + renderValidationScope;
      }
      validationMessage += ' | metric ' + renderMetricCount + ' รายการ';
    } else if (renderMetricCount > 0) {
      validationStatus = 'Partial';
      validationMessage = 'ตรวจ snapshot key ได้ แต่ metric ของชั้นพิมพ์ยังไม่ครบตามประเภทรายงาน';
      if (renderValidationScope) {
        validationMessage += ' | ขอบเขต: ' + renderValidationScope;
      }
      if (missingMetricKeys.length > 0) {
        validationMessage += ' | ขาด: ' + missingMetricKeys.join(', ');
      }
    } else {
      validationStatus = 'Partial';
      validationMessage = 'ตรวจ snapshot key ได้ แต่ยังไม่มี metric เพียงพอสำหรับยืนยันรายละเอียด DOM ครบทุกจุด';
      if (renderValidationScope) {
        validationMessage += ' | ขอบเขต: ' + renderValidationScope;
      }
    }
  }

  return {
    renderSnapshotKey: renderSnapshotKey,
    renderSnapshotSummary: renderSnapshotSummary,
    renderAsOfLabel: renderAsOfLabel,
    validationStatus: validationStatus,
    validationMessage: validationMessage,
    validatedAt: getThaiDate(new Date()).dateTime,
    validationScope: renderValidationScope,
    renderMetricCount: renderMetricCount,
    renderMetricKeys: renderMetricKeys
  };
}

function ensureReportArchivePeriodFolder_(ss, meta) {
  var rootFolder = getReportArchiveRootFolder_(ss);
  var typeFolder = getOrCreateChildFolderByName_(rootFolder, meta.reportTypeLabel);
  var yearFolder = getOrCreateChildFolderByName_(typeFolder, String(meta.yearBe || 'ไม่ระบุปี'));
  var monthFolder = getOrCreateChildFolderByName_(yearFolder, padNumber_(meta.month || 1, 2) + '-' + getThaiMonthLabelByNumber_(meta.month));
  return {
    folder: monthFolder,
    path: [rootFolder.getName(), typeFolder.getName(), yearFolder.getName(), monthFolder.getName()].join(' / ')
  };
}

function findReportArchiveIndexRowByKey_(sheet, archiveKey) {
  if (!sheet || !archiveKey || sheet.getLastRow() <= 1) return 0;
  var matchedRows = findRowsByExactValueInColumn_(sheet, 1, String(archiveKey), 2);
  if (!matchedRows.length) return 0;
  return matchedRows[matchedRows.length - 1];
}

function readReportArchiveRecords_(sheet) {
  var records = [];
  if (!sheet || sheet.getLastRow() <= 1) return records;

  var rows = sheet.getRange(2, 1, sheet.getLastRow() - 1, getReportArchiveIndexHeaders_().length).getValues();
  for (var i = 0; i < rows.length; i++) {
    var row = rows[i];
    var record = {
      archiveKey: String(row[0] || '').trim(),
      documentNo: String(row[1] || '').trim(),
      reportType: String(row[2] || '').trim(),
      reportTypeLabel: String(row[3] || '').trim(),
      yearBe: Number(row[4]) || 0,
      month: Number(row[5]) || 0,
      day: Number(row[6]) || 0,
      periodLabel: String(row[7] || '').trim(),
      reportDateDisplay: String(row[8] || '').trim(),
      memberId: String(row[9] || '').trim(),
      memberName: String(row[10] || '').trim(),
      title: String(row[11] || '').trim(),
      fileName: String(row[12] || '').trim(),
      driveFileId: String(row[13] || '').trim(),
      url: String(row[14] || '').trim(),
      folderPath: String(row[15] || '').trim(),
      createdAt: String(row[16] || '').trim(),
      createdBy: String(row[17] || '').trim(),
      status: String(row[18] || '').trim() || 'Active',
      snapshotKey: String(row[19] || '').trim(),
      snapshotSummary: String(row[20] || '').trim(),
      asOfLabel: String(row[21] || '').trim(),
      renderSnapshotKey: String(row[22] || '').trim(),
      renderSnapshotSummary: String(row[23] || '').trim(),
      renderAsOfLabel: String(row[24] || '').trim(),
      validationStatus: String(row[25] || '').trim(),
      validationMessage: String(row[26] || '').trim(),
      validatedAt: String(row[27] || '').trim(),
      validationScope: String(row[28] || '').trim(),
      renderMetricCount: Number(row[29]) || 0,
      renderMetricKeys: String(row[30] || '').trim()
    };
    if (!record.archiveKey || !record.driveFileId) continue;
    records.push(record);
  }
  return records;
}

function saveReportPdfArchive(payloadStr) {
  requirePermission_('reports.archive', 'ไม่มีสิทธิ์บันทึกรายงาน PDF');

  var lock = LockService.getScriptLock();
  var lockAcquired = false;
  try {
    lock.waitLock(30000);
    lockAcquired = true;

    var ss = getDatabaseSpreadsheet_();
    var payload = typeof payloadStr === 'string' ? JSON.parse(payloadStr) : (payloadStr || {});
    var pdfBase64 = String(payload.pdfBase64 || '').trim();
    if (!pdfBase64) {
      throw new Error('ไม่พบข้อมูลไฟล์ PDF สำหรับบันทึก');
    }

    var meta = normalizeReportArchivePayload_(payload);
    if (meta.reportType === 'overdue_interest' && !String(meta.reportDateDisplay || '').trim()) {
      throw new Error('ไม่พบวันที่อ้างอิงของรายงานหนี้คงค้าง');
    }
    var snapshot = payload && typeof payload.snapshot === 'object' && payload.snapshot
      ? payload.snapshot
      : buildReportSnapshotForArchivePayload_(payload);
    var snapshotMeta = buildReportSnapshotKeyAndSummary_(meta, payload, snapshot);
    var validationMeta = validateRenderSnapshotPayload_(payload, snapshotMeta, meta.reportType);
    var documentNo = generateDocumentReference_('RPT', new Date(), ss);
    if (String(meta.fileName || '').indexOf(documentNo) !== 0) {
      meta.fileName = documentNo + ' - ' + String(meta.fileName || 'report.pdf');
    }
    var folderInfo = ensureReportArchivePeriodFolder_(ss, meta);
    var pdfBlob = Utilities.newBlob(Utilities.base64Decode(pdfBase64), MimeType.PDF, meta.fileName);
    var file = folderInfo.folder.createFile(pdfBlob);

    var indexSheet = ensureReportArchiveIndexSheet_(ss);
    var existingRow = findReportArchiveIndexRowByKey_(indexSheet, meta.archiveKey);
    var actorName = getCurrentActorName_();
    var createdAt = getThaiDate(new Date()).dateTime;
    var rowValues = [[
      meta.archiveKey,
      documentNo,
      meta.reportType,
      meta.reportTypeLabel,
      meta.yearBe,
      meta.month,
      meta.day,
      meta.periodLabel,
      meta.reportDateDisplay,
      meta.memberId,
      meta.memberName,
      meta.title,
      meta.fileName,
      file.getId(),
      file.getUrl(),
      folderInfo.path,
      createdAt,
      actorName,
      'Active',
      snapshotMeta.snapshotKey,
      snapshotMeta.snapshotSummary,
      snapshotMeta.asOfLabel,
      validationMeta.renderSnapshotKey,
      validationMeta.renderSnapshotSummary,
      validationMeta.renderAsOfLabel,
      validationMeta.validationStatus,
      validationMeta.validationMessage,
      validationMeta.validatedAt,
      validationMeta.validationScope,
      validationMeta.renderMetricCount,
      validationMeta.renderMetricKeys
    ]];

    if (existingRow > 1) {
      var previousFileId = String(indexSheet.getRange(existingRow, 14).getValue() || '').trim();
      if (previousFileId && previousFileId !== file.getId()) {
        try {
          DriveApp.getFileById(previousFileId).setTrashed(true);
        } catch (trashError) {}
      }
      indexSheet.getRange(existingRow, 1, 1, rowValues[0].length).setValues(rowValues);
    } else {
      appendRowsSafely_(indexSheet, rowValues, 'เพิ่มสารบัญรายงาน PDF');
    }

    appendAuditLogSafe_(ss, {
      action: 'SAVE_REPORT_PDF_ARCHIVE',
      entityType: 'REPORT_ARCHIVE',
      entityId: meta.archiveKey,
      referenceNo: documentNo,
      after: { reportType: meta.reportType, periodLabel: meta.periodLabel, fileName: meta.fileName, driveFileId: file.getId() },
      details: 'บันทึกรายงาน PDF เข้าคลังสำเร็จ'
    });
    logSystemActionFast(
      ss,
      'บันทึกรายงาน PDF',
      meta.reportTypeLabel + ' | ' + meta.periodLabel + ' | ' + meta.fileName + ' | ' + file.getId()
    );

    return {
      status: 'Success',
      archiveKey: meta.archiveKey,
      documentNo: documentNo,
      reportType: meta.reportType,
      reportTypeLabel: meta.reportTypeLabel,
      title: meta.title,
      fileName: meta.fileName,
      driveFileId: file.getId(),
      url: file.getUrl(),
      folderPath: folderInfo.path,
      createdAt: createdAt,
      createdBy: actorName,
      snapshotKey: snapshotMeta.snapshotKey,
      snapshotSummary: snapshotMeta.snapshotSummary,
      asOfLabel: snapshotMeta.asOfLabel,
      renderSnapshotKey: validationMeta.renderSnapshotKey,
      renderSnapshotSummary: validationMeta.renderSnapshotSummary,
      renderAsOfLabel: validationMeta.renderAsOfLabel,
      validationStatus: validationMeta.validationStatus,
      validationMessage: validationMeta.validationMessage,
      validatedAt: validationMeta.validatedAt,
      validationScope: validationMeta.validationScope,
      renderMetricCount: validationMeta.renderMetricCount,
      renderMetricKeys: validationMeta.renderMetricKeys,
      message: 'บันทึกรายงาน PDF เข้าคลังเรียบร้อยแล้ว'
    };
  } catch (e) {
    console.error('saveReportPdfArchive failed', e);
    return { status: 'Error', message: 'บันทึกรายงาน PDF ไม่สำเร็จ: ' + e.toString() };
  } finally {
    if (lockAcquired) lock.releaseLock();
  }
}

function listReportPdfArchives(filterStr) {
  requireAuthenticatedSession_();
  try {
    var filter = typeof filterStr === 'string' ? JSON.parse(filterStr) : (filterStr || {});
    var reportTypeFilter = String(filter.reportType || 'all').trim();
    var yearBeFilter = String(filter.yearBe || 'all').trim();
    var monthFilter = String(filter.month || 'all').trim();
    var cacheKey = 'reportPdfArchives::' + [reportTypeFilter || 'all', yearBeFilter || 'all', monthFilter || 'all'].join('::');
    var cached = getJsonCache_(cacheKey);
    if (cached && Array.isArray(cached.records)) {
      return { status: 'Success', records: cached.records, cached: true };
    }

    var ss = getDatabaseSpreadsheet_();
    var indexSheet = ensureReportArchiveIndexSheet_(ss);
    var records = readReportArchiveRecords_(indexSheet).filter(function(record) {
      if (record.status && record.status !== 'Active') return false;
      if (reportTypeFilter && reportTypeFilter !== 'all' && record.reportType !== reportTypeFilter) return false;
      if (yearBeFilter && yearBeFilter !== 'all' && String(record.yearBe || '') !== yearBeFilter) return false;
      if (monthFilter && monthFilter !== 'all' && String(record.month || '') !== monthFilter) return false;
      return true;
    });

    records.sort(function(a, b) {
      var sortKeyA = buildThaiDateTimeSortKey_(a.createdAt);
      var sortKeyB = buildThaiDateTimeSortKey_(b.createdAt);
      if (sortKeyA !== sortKeyB) return sortKeyB - sortKeyA;
      return String(b.documentNo || '').localeCompare(String(a.documentNo || ''));
    });

    var payload = {
      status: 'Success',
      records: records,
      cached: false
    };
    putJsonCache_(cacheKey, { records: records }, 30);
    return payload;
  } catch (e) {
    console.error('listReportPdfArchives failed', e);
    return { status: 'Error', message: 'โหลดคลังรายงาน PDF ไม่สำเร็จ: ' + e.toString(), records: [] };
  }
}

function findLatestReportPdfArchiveBySelection(payloadStr) {
  requireAuthenticatedSession_();
  try {
    var payload = typeof payloadStr === 'string' ? JSON.parse(payloadStr) : (payloadStr || {});
    var meta = normalizeReportArchivePayload_(payload);
    var cacheKey = 'reportPdfArchiveMatch::' + String(meta.archiveKey || '').trim();
    var cached = getJsonCache_(cacheKey);
    if (cached && Object.prototype.hasOwnProperty.call(cached, 'record')) {
      return {
        status: 'Success',
        record: cached.record || null,
        archiveKey: meta.archiveKey,
        cached: true
      };
    }

    var ss = getDatabaseSpreadsheet_();
    var indexSheet = ensureReportArchiveIndexSheet_(ss);
    var records = readReportArchiveRecords_(indexSheet).filter(function(record) {
      return record.status === 'Active' && record.archiveKey === meta.archiveKey;
    });

    records.sort(function(a, b) {
      var sortKeyA = buildThaiDateTimeSortKey_(a.createdAt);
      var sortKeyB = buildThaiDateTimeSortKey_(b.createdAt);
      if (sortKeyA !== sortKeyB) return sortKeyB - sortKeyA;
      return String(b.documentNo || '').localeCompare(String(a.documentNo || ''));
    });

    var latestRecord = records.length ? records[0] : null;
    putJsonCache_(cacheKey, { record: latestRecord }, 30);
    return {
      status: 'Success',
      record: latestRecord,
      archiveKey: meta.archiveKey,
      cached: false
    };
  } catch (e) {
    console.error('findLatestReportPdfArchiveBySelection failed', e);
    return { status: 'Error', message: 'ค้นหารายงาน PDF ที่บันทึกไว้ไม่สำเร็จ: ' + e.toString(), record: null };
  }
}


function normalizeReconciliationPeriodInput_(periodInput) {
  var raw = periodInput;
  if (typeof raw === 'string') {
    var text = String(raw || '').trim();
    if (text) {
      try {
        raw = JSON.parse(text);
      } catch (e) {
        raw = { reportDate: text };
      }
    } else {
      raw = {};
    }
  }
  if (!raw || typeof raw !== 'object') raw = {};

  var reportDate = String(raw.reportDate || raw.date || '').trim();
  var parsedDate = reportDate ? parseReportDateInput_(reportDate) : null;
  var now = new Date();
  var nowParts = parseThaiDateParts_(now) || { yearBe: now.getFullYear() + 543, month: now.getMonth() + 1, dateOnly: getThaiDate(now).dateOnly };
  var yearBe = Number(raw.yearBe) || Number(parsedDate && parsedDate.yearBe) || Number(nowParts.yearBe) || (now.getFullYear() + 543);
  var month = Number(raw.month) || Number(parsedDate && parsedDate.month) || Number(nowParts.month) || (now.getMonth() + 1);
  if (month < 1 || month > 12) month = Number(nowParts.month) || 1;
  if (!yearBe) yearBe = Number(nowParts.yearBe) || (now.getFullYear() + 543);

  return {
    yearBe: yearBe,
    month: month,
    monthKey: String(yearBe) + '-' + padNumber_(month, 2),
    reportDate: reportDate,
    reportDateDisplay: parsedDate ? parsedDate.dateOnly : ''
  };
}

function collectActiveTransactionRecordsForMonth_(ss, yearBe, month) {
  var spreadsheet = ss || getDatabaseSpreadsheet_();
  var sheets = listCurrentTransactionSheets_(spreadsheet).concat(listTransactionArchiveSheetsByYear_(spreadsheet, yearBe, month));
  var records = [];
  var seen = {};
  for (var s = 0; s < sheets.length; s++) {
    var sheet = sheets[s];
    var activeRecords = getActiveTransactionRecordsFromSheetCached_(sheet);
    for (var r = 0; r < activeRecords.length; r++) {
      var record = activeRecords[r];
      if (Number(record.yearBe) !== Number(yearBe) || Number(record.month) !== Number(month)) continue;
      var dedupeKey = String(record.id || '').trim() || [record.timestamp, record.contract, record.memberId, record.principalPaid, record.interestPaid, record.newBalance, record.note, record.sourceSheet].join('|');
      if (seen[dedupeKey]) continue;
      seen[dedupeKey] = true;
      records.push(record);
    }
  }
  records.sort(compareTransactionRecordOrderAsc_);
  return records;
}

function summarizeTransactionRecords_(records) {
  var summary = { count: 0, principalPaid: 0, interestPaid: 0, totalPaid: 0, uniqueContracts: 0, uniqueMembers: 0, activeDays: 0 };
  var contractMap = {};
  var memberMap = {};
  var dateMap = {};
  (records || []).forEach(function(record) {
    summary.count += 1;
    summary.principalPaid += Number(record.principalPaid) || 0;
    summary.interestPaid += Number(record.interestPaid) || 0;
    if (record.contract) contractMap[String(record.contract)] = true;
    if (record.memberId) memberMap[String(record.memberId)] = true;
    if (record.dateOnly) dateMap[String(record.dateOnly)] = true;
  });
  summary.totalPaid = summary.principalPaid + summary.interestPaid;
  summary.uniqueContracts = Object.keys(contractMap).length;
  summary.uniqueMembers = Object.keys(memberMap).length;
  summary.activeDays = Object.keys(dateMap).length;
  return summary;
}

function runReconciliation(periodInput) {
  requirePermission_('reports.reconcile', 'ไม่มีสิทธิ์ตรวจความสอดคล้องของยอด');
  requirePermission_('reports.view', 'ไม่มีสิทธิ์ดูรายงานประกอบการตรวจสอบ');

  var ss = ensureRuntimeDatabase_();
  var period = normalizeReconciliationPeriodInput_(periodInput);
  var monthRecords = collectActiveTransactionRecordsForMonth_(ss, period.yearBe, period.month);
  var transactionSummary = summarizeTransactionRecords_(monthRecords);
  var currentRecords = monthRecords.filter(function(record) { return String(record.sourceSheet || '') === 'Transactions'; });
  var archiveRecords = monthRecords.filter(function(record) { return String(record.sourceSheet || '') !== 'Transactions'; });
  var currentSummary = summarizeTransactionRecords_(currentRecords);
  var archiveSummary = summarizeTransactionRecords_(archiveRecords);

  var uniqueDates = {};
  for (var i = 0; i < monthRecords.length; i++) {
    if (monthRecords[i].dateOnly) uniqueDates[monthRecords[i].dateOnly] = true;
  }
  var dateList = Object.keys(uniqueDates).sort(function(a, b) {
    return getThaiDateNumber_(a) - getThaiDateNumber_(b);
  });

  var reportSummary = { dayCount: 0, principalPaid: 0, interestPaid: 0, offsetPaid: 0, totalPaid: 0, cfwBalance: 0 };
  var reportErrors = [];
  for (var d = 0; d < dateList.length; d++) {
    var reportResult = getDailyReport(dateList[d]);
    if (!reportResult || reportResult.status === 'Error') {
      reportErrors.push({ date: dateList[d], message: String((reportResult && reportResult.message) || 'โหลดรายงานส่งบัญชีไม่สำเร็จ') });
      continue;
    }
    reportSummary.dayCount += 1;
    reportSummary.principalPaid += Number(reportResult.summary && reportResult.summary.principalPaid) || 0;
    reportSummary.interestPaid += Number(reportResult.summary && reportResult.summary.interestPaid) || 0;
    reportSummary.offsetPaid += Number(reportResult.summary && reportResult.summary.offsetPaid) || 0;
    reportSummary.totalPaid += Number(reportResult.summary && reportResult.summary.totalPaid) || 0;
    reportSummary.cfwBalance += Number(reportResult.summary && reportResult.summary.cfwBalance) || 0;
  }

  var mismatchItems = [];
  var principalDiff = Math.round(((transactionSummary.principalPaid || 0) - (reportSummary.principalPaid || 0)) * 100) / 100;
  var interestDiff = Math.round(((transactionSummary.interestPaid || 0) - (reportSummary.interestPaid || 0)) * 100) / 100;
  if (principalDiff !== 0) mismatchItems.push({ severity: 'error', code: 'PRINCIPAL_DIFF', label: 'ผลรวมเงินต้นจากธุรกรรมไม่ตรงกับรายงานส่งบัญชีรวมเดือน', value: principalDiff });
  if (interestDiff !== 0) mismatchItems.push({ severity: 'error', code: 'INTEREST_DIFF', label: 'ผลรวมดอกเบี้ยจากธุรกรรมไม่ตรงกับรายงานส่งบัญชีรวมเดือน', value: interestDiff });
  if (currentSummary.count > 0) mismatchItems.push({ severity: 'warning', code: 'PENDING_BACKUP', label: 'ยังมีรายการค้างอยู่ในชีต Transactions (ยังไม่ได้ Backup ลงชีตรายเดือน)', value: currentSummary.count });
  if (reportErrors.length > 0) mismatchItems.push({ severity: 'warning', code: 'REPORT_READ_ERROR', label: 'มีบางวันโหลดรายงานส่งบัญชีไม่สำเร็จ', value: reportErrors.length });

  var anomalies = [];
  var loanSheet = ss.getSheetByName('Loans');
  var loanRecordsByContract = {};
  if (loanSheet && loanSheet.getLastRow() > 1) {
    var loanRows = loanSheet.getRange(2, 1, loanSheet.getLastRow() - 1, 12).getValues();
    for (var lr = 0; lr < loanRows.length; lr++) {
      var loanSnapshot = buildAuditLoanSnapshotFromRow_(loanRows[lr]);
      loanRecordsByContract[loanSnapshot.contract] = loanSnapshot;
      if (loanSnapshot.balance < 0) anomalies.push({ type: 'loan', severity: 'error', key: loanSnapshot.contract, label: 'สัญญามียอดคงเหลือติดลบ', detail: loanSnapshot.contract + ' | ' + loanSnapshot.memberName, amount: loanSnapshot.balance });
      if (loanSnapshot.balance <= 0 && String(loanSnapshot.status || '').indexOf('ปิดบัญชี') === -1) anomalies.push({ type: 'loan', severity: 'warning', key: loanSnapshot.contract, label: 'สัญญายอดคงเหลือเป็นศูนย์แต่สถานะยังไม่ปิดบัญชี', detail: loanSnapshot.contract + ' | ' + loanSnapshot.memberName, amount: loanSnapshot.balance });
      if (loanSnapshot.balance > 0 && String(loanSnapshot.status || '').indexOf('ปิดบัญชี') !== -1) anomalies.push({ type: 'loan', severity: 'error', key: loanSnapshot.contract, label: 'สัญญายังมียอดคงเหลือแต่สถานะเป็นปิดบัญชี', detail: loanSnapshot.contract + ' | ' + loanSnapshot.memberName, amount: loanSnapshot.balance });
    }
  }

  for (var t = 0; t < monthRecords.length; t++) {
    var tx = monthRecords[t];
    if (!tx.contract) {
      anomalies.push({ type: 'transaction', severity: 'error', key: tx.id || ('row-' + t), label: 'ธุรกรรมไม่มีเลขที่สัญญา', detail: tx.timestamp + ' | ' + (tx.memberName || tx.memberId || '-'), amount: (Number(tx.principalPaid) || 0) + (Number(tx.interestPaid) || 0) });
      continue;
    }
    if (!loanRecordsByContract[tx.contract]) anomalies.push({ type: 'transaction', severity: 'warning', key: tx.id || tx.contract, label: 'ธุรกรรมอ้างอิงสัญญาที่ไม่พบใน Loans', detail: tx.contract + ' | ' + tx.timestamp, amount: (Number(tx.principalPaid) || 0) + (Number(tx.interestPaid) || 0) });
    if ((Number(tx.principalPaid) || 0) < 0 || (Number(tx.interestPaid) || 0) < 0) anomalies.push({ type: 'transaction', severity: 'error', key: tx.id || tx.contract, label: 'ธุรกรรมติดลบ', detail: tx.contract + ' | ' + tx.timestamp, amount: (Number(tx.principalPaid) || 0) + (Number(tx.interestPaid) || 0) });
  }

  appendAuditLogSafe_(ss, {
    action: 'RUN_RECONCILIATION',
    entityType: 'REPORT',
    entityId: period.monthKey,
    referenceNo: period.monthKey,
    before: {},
    after: { transactionSummary: transactionSummary, reportSummary: reportSummary, mismatchCount: mismatchItems.length, anomalyCount: anomalies.length },
    reason: '',
    details: 'ตรวจความสอดคล้องของยอดประจำเดือน ' + period.monthKey
  });

  return {
    status: 'Success',
    period: period,
    transactionSummary: transactionSummary,
    currentSummary: currentSummary,
    archiveSummary: archiveSummary,
    reportSummary: reportSummary,
    mismatchItems: mismatchItems,
    anomalies: anomalies.slice(0, 200),
    anomalyCount: anomalies.length,
    reportErrors: reportErrors,
    archiveSheetNames: listTransactionArchiveSheetsByYear_(ss, period.yearBe, period.month).map(function(sheet) { return String(sheet.getName() || ''); })
  };
}

function getDailyReport(reportDateInput) {
  requirePermission_('reports.view', 'ไม่มีสิทธิ์ดูรายงาน');
  try {
    var reportDateParts = parseReportDateInput_(reportDateInput);
    if (!reportDateParts) {
      return { status: 'Error', message: 'รูปแบบวันที่รายงานไม่ถูกต้อง' };
    }

    var reportDateOnly = reportDateParts.dateOnly;
    var reportDateNumber = getThaiDateNumberFromParts_(reportDateParts);
    var cacheKey = 'dailyReport::' + reportDateOnly;
    var cached = getJsonCache_(cacheKey);
    if (cached && cached.summary && Array.isArray(cached.details)) {
      return {
        status: 'Success',
        reportDate: reportDateOnly,
        summary: cached.summary,
        details: cached.details,
        mixedCaseNotes: cached.mixedCaseNotes || [],
        cached: true
      };
    }

    var ss = ensureRuntimeDatabase_();

    var loanSheet = getSheetOrThrow(ss, 'Loans');
    var loanRows = [];
    if (loanSheet.getLastRow() > 1) {
      loanRows = loanSheet.getRange(2, 1, loanSheet.getLastRow() - 1, 12).getValues();
    }

    var indexedRecords = getActiveIndexedTransactionRecordsCached_(ss, 0);
    var dailyFactsByContract = {};
    var seenIds = {};

    for (var r = 0; r < indexedRecords.length; r++) {
      var record = indexedRecords[r];
      var contractKey = String(record.contract || '').trim();
      if (!contractKey) continue;

      var dedupeKey = String(record.id || '').trim() || [
        record.timestamp,
        record.contract,
        record.memberId,
        record.principalPaid,
        record.interestPaid,
        record.newBalance,
        record.note,
        record.sourceSheet,
        String(record.rowIndex || 0)
      ].join('|');
      if (seenIds[dedupeKey]) continue;
      seenIds[dedupeKey] = true;

      if (!dailyFactsByContract[contractKey]) {
        dailyFactsByContract[contractKey] = {
          principalPaidAfterReportDate: 0,
          hasTransactionOnReportDate: false,
          txToday: [],
          latestRecordOnOrBeforeDate: null,
          lastBalanceOnOrBeforeDate: null
        };
      }

      var contractFacts = dailyFactsByContract[contractKey];
      if (record.dateNumber > reportDateNumber) {
        contractFacts.principalPaidAfterReportDate += Number(record.principalPaid) || 0;
      } else if (!contractFacts.latestRecordOnOrBeforeDate || compareTransactionRecordOrderAsc_(contractFacts.latestRecordOnOrBeforeDate, record) <= 0) {
        contractFacts.latestRecordOnOrBeforeDate = record;
        contractFacts.lastBalanceOnOrBeforeDate = Number(record.newBalance) || 0;
      }
      if (record.dateNumber === reportDateNumber) {
        contractFacts.hasTransactionOnReportDate = true;
        contractFacts.txToday.push(record);
      }
    }

    var dailyFactContracts = Object.keys(dailyFactsByContract);
    for (var factIndex = 0; factIndex < dailyFactContracts.length; factIndex++) {
      var factKey = dailyFactContracts[factIndex];
      var factEntry = dailyFactsByContract[factKey];
      if (factEntry && factEntry.txToday && factEntry.txToday.length > 1) {
        factEntry.txToday.sort(compareTransactionRecordOrderAsc_);
      }
    }

    var activeLoansOnDate = [];
    for (var i = 0; i < loanRows.length; i++) {
      var row = loanRows[i];
      var contractNo = String(row[1] || '').trim();
      if (!contractNo) continue;

      var createdAtNumber = getLoanCreatedAtNumberForReport_(row);
      if (!createdAtNumber || createdAtNumber > reportDateNumber) {
        continue;
      }

      var contractFacts = dailyFactsByContract[contractNo] || {
        principalPaidAfterReportDate: 0,
        hasTransactionOnReportDate: false,
        txToday: [],
        latestRecordOnOrBeforeDate: null,
        lastBalanceOnOrBeforeDate: null
      };

      var balanceAtReportDate = getHistoricalBalanceAtReportDate_(row, contractFacts.principalPaidAfterReportDate, contractFacts.lastBalanceOnOrBeforeDate);
      if (balanceAtReportDate <= 0 && !contractFacts.hasTransactionOnReportDate) {
        continue;
      }

      activeLoansOnDate.push({
        memberId: String(row[0] || '').trim(),
        contract: contractNo,
        member: String(row[2] || '').trim(),
        balanceAtReportDate: balanceAtReportDate,
        txToday: contractFacts.txToday || []
      });
    }

    activeLoansOnDate.sort(function(a, b) {
      var idA = parseInt(a.memberId, 10);
      var idB = parseInt(b.memberId, 10);
      var valA = isNaN(idA) ? a.memberId : idA;
      var valB = isNaN(idB) ? b.memberId : idB;
      if (valA < valB) return -1;
      if (valA > valB) return 1;
      var balanceA = Number(a.balanceAtReportDate) || 0;
      var balanceB = Number(b.balanceAtReportDate) || 0;
      if (balanceA !== balanceB) return balanceB - balanceA;
      if (!a.contract) return 1;
      if (!b.contract) return -1;
      return String(a.contract).localeCompare(String(b.contract));
    });

    var summary = { bfwBalance: 0, principalPaid: 0, offsetPaid: 0, interestPaid: 0, totalPaid: 0, cfwBalance: 0 };
    var details = [];
    var mixedCaseNotes = [];

    for (var k = 0; k < activeLoansOnDate.length; k++) {
      var loan = activeLoansOnDate[k];
      var txToday = loan.txToday || [];

      var cashPrincipalPaidToday = 0;
      var offsetPrincipalPaidToday = 0;
      var totalPrincipalPaidToday = 0;
      var interestPaidToday = 0;
      var note = '';
      var isOverdue = false;
      var isOffset = false;
      var hasCashPrincipal = false;
      var hasInterest = false;
      var hasOffsetPrincipal = false;
      var transactionBreakdown = [];
      var mixedCaseDetail = '';

      if (txToday.length > 0) {
        var noteParts = [];
        var noteSeen = {};
        for (var y = 0; y < txToday.length; y++) {
          var txItem = txToday[y];
          var txPrincipalPaid = Number(txItem.principalPaid) || 0;
          var txInterestPaid = Number(txItem.interestPaid) || 0;
          var noteText = String(txItem.note || '').trim();
          var txIsOffset = noteText && noteText.indexOf('กลบหนี้') !== -1;

          totalPrincipalPaidToday += txPrincipalPaid;
          interestPaidToday += txInterestPaid;
          if (txIsOffset) {
            offsetPrincipalPaidToday += txPrincipalPaid;
          } else {
            cashPrincipalPaidToday += txPrincipalPaid;
          }

          if (txPrincipalPaid > 0 && !txIsOffset) hasCashPrincipal = true;
          if (txInterestPaid > 0) hasInterest = true;
          if (txPrincipalPaid > 0 && txIsOffset) hasOffsetPrincipal = true;
          if (txIsOffset) isOffset = true;

          if (noteText && noteText !== '-' && !noteSeen[noteText]) {
            noteParts.push(noteText);
            noteSeen[noteText] = true;
          }

          transactionBreakdown.push({
            timestamp: txItem.timestamp || txItem.dateOnly || '',
            principalPaid: txPrincipalPaid,
            interestPaid: txInterestPaid,
            newBalance: Number(txItem.newBalance) || 0,
            note: noteText || '-'
          });
        }

        if (hasCashPrincipal && hasOffsetPrincipal) {
          note = 'ชำระปกติ + กลบหนี้';
        } else if (isOffset) {
          note = 'กลบหนี้';
        } else {
          note = noteParts.join(', ');
        }
        if (!isOffset && loan.balanceAtReportDate <= 0) note = 'ปิดบัญชีวันนี้';

        if (hasCashPrincipal && hasInterest && hasOffsetPrincipal) {
          var lines = [];
          for (var z = 0; z < transactionBreakdown.length; z++) {
            var txLine = transactionBreakdown[z];
            lines.push(
              (z + 1) + ') ' + (txLine.timestamp || '-') +
              ' | ชำระต้น ' + formatCurrency_(txLine.principalPaid) +
              ' | ชำระดอก ' + formatCurrency_(txLine.interestPaid) +
              ' | คงเหลือ ' + formatCurrency_(txLine.newBalance) +
              ' | ' + (txLine.note || '-')
            );
          }
          mixedCaseDetail = lines.join('\n');
          mixedCaseNotes.push({
            memberId: loan.memberId || '-',
            name: loan.member || '-',
            contract: loan.contract || '-',
            detail: mixedCaseDetail
          });
        }
      } else if (loan.balanceAtReportDate > 0) {
        note = 'ขาดส่ง';
        isOverdue = true;
      }

      var balanceBeforeToday = loan.balanceAtReportDate + totalPrincipalPaidToday;
      if (balanceBeforeToday <= 0 && loan.balanceAtReportDate <= 0 && totalPrincipalPaidToday === 0 && interestPaidToday === 0) {
        continue;
      }

      summary.bfwBalance += balanceBeforeToday;
      summary.principalPaid += cashPrincipalPaidToday;
      summary.offsetPaid += offsetPrincipalPaidToday;
      summary.interestPaid += interestPaidToday;
      summary.cfwBalance += loan.balanceAtReportDate;

      details.push({
        no: details.length + 1,
        memberId: loan.memberId || '-',
        name: loan.member || '-',
        broughtForward: balanceBeforeToday,
        principalPaid: cashPrincipalPaidToday,
        offsetPaid: offsetPrincipalPaidToday,
        totalPrincipalReduction: totalPrincipalPaidToday,
        displayPrincipalPaid: totalPrincipalPaidToday,
        interestPaid: interestPaidToday,
        balance: loan.balanceAtReportDate,
        note: note,
        isOverdue: isOverdue,
        isOffset: isOffset,
        contract: loan.contract || '',
        mixedCaseDetail: mixedCaseDetail,
        transactionBreakdown: transactionBreakdown
      });
    }

    summary.totalPaid = summary.principalPaid + summary.interestPaid;

    var payload = {
      status: 'Success',
      reportDate: reportDateOnly,
      summary: summary,
      details: details,
      mixedCaseNotes: mixedCaseNotes,
      cached: false
    };
    putJsonCache_(cacheKey, {
      summary: summary,
      details: details,
      mixedCaseNotes: mixedCaseNotes
    }, 120);
    return payload;
  } catch (e) {
    return {
      status: 'Error',
      message: 'โหลดรายงานส่งบัญชีไม่สำเร็จ: ' + e.toString()
    };
  }
}

function padNumber_(num, size) {
  var s = String(num);
  while (s.length < size) s = '0' + s;
  return s;
}


function formatCurrency_(value) {
  var num = Number(value || 0);
  if (!isFinite(num)) num = 0;
  var sign = num < 0 ? '-' : '';
  var abs = Math.abs(num);
  var parts = abs.toFixed(2).split('.');
  parts[0] = parts[0].replace(/\B(?=(\d{3})+(?!\d))/g, ',');
  return sign + parts.join('.');
}

function formatNumberForDisplay_(value) {
  var num = Number(value || 0);
  if (!isFinite(num)) num = 0;

  var sign = num < 0 ? '-' : '';
  var abs = Math.abs(num);
  var rounded = Math.round(abs * 100) / 100;
  var hasFraction = Math.abs(rounded % 1) > 0;
  var parts = rounded.toFixed(hasFraction ? 2 : 0).split('.');

  parts[0] = parts[0].replace(/\B(?=(\d{3})+(?!\d))/g, ',');

  return sign + parts.join('.');
}

function addCommas(value) {
  return formatNumberForDisplay_(value);
}


// ==========================================
// PHASE 5: MAKER-CHECKER + INTERNAL NOTES
// ==========================================
var APPROVAL_QUEUE_SHEET_NAME = 'ApprovalQueue';
var APPROVAL_QUEUE_HEADERS = [
  'RequestId', 'RequestedAt', 'RequestedBy', 'RequestedByName', 'ActionKey',
  'EntityType', 'EntityId', 'PayloadJson', 'Reason', 'Status',
  'DecisionBy', 'DecisionAt', 'DecisionNote', 'ResultRef', 'ResultJson'
];
var INTERNAL_NOTES_SHEET_NAME = 'InternalNotes';
var INTERNAL_NOTES_HEADERS = [
  'NoteId', 'EntityType', 'EntityId', 'NoteText', 'Status',
  'CreatedBy', 'CreatedByName', 'CreatedAt', 'UpdatedBy', 'UpdatedAt'
];
var APPROVAL_ACTION_CATALOG = {
  reverse_payment: { label: 'กลับรายการรับชำระ', entityType: 'TRANSACTION' },
  close_accounting: { label: 'ปิดยอดบัญชี', entityType: 'SYSTEM' },
  backup_transactions: { label: 'Backup ธุรกรรม', entityType: 'SYSTEM' }
};

(function extendPhase5Permissions_() {
  if (typeof PERMISSION_CATALOG === 'undefined' || !PERMISSION_CATALOG || !PERMISSION_CATALOG.push) return;
  var additionalPermissions = [
    { key: 'notes.view', label: 'ดูหมายเหตุภายใน' },
    { key: 'notes.manage', label: 'เพิ่ม/ลบหมายเหตุภายใน' },
    { key: 'approvals.request', label: 'ส่งคำขออนุมัติรายการสำคัญ' },
    { key: 'approvals.view', label: 'ดูคิวคำขออนุมัติ' },
    { key: 'approvals.manage', label: 'อนุมัติ/ปฏิเสธคำขอ' }
  ];
  for (var i = 0; i < additionalPermissions.length; i++) {
    var item = additionalPermissions[i];
    var exists = false;
    for (var j = 0; j < PERMISSION_CATALOG.length; j++) {
      if (String(PERMISSION_CATALOG[j].key || '').trim() === item.key) {
        exists = true;
        break;
      }
    }
    if (!exists) PERMISSION_CATALOG.push(item);
  }
})();

var __phase5_orig_getDefaultPermissionMapByRole_ = getDefaultPermissionMapByRole_;
getDefaultPermissionMapByRole_ = function(role) {
  var map = __phase5_orig_getDefaultPermissionMapByRole_(role);
  var normalizedRole = normalizeUserRole_(role);
  if (normalizedRole === 'admin') return map;
  map['notes.view'] = true;
  map['notes.manage'] = true;
  map['approvals.request'] = true;
  if (!Object.prototype.hasOwnProperty.call(map, 'approvals.view')) map['approvals.view'] = false;
  if (!Object.prototype.hasOwnProperty.call(map, 'approvals.manage')) map['approvals.manage'] = false;
  return map;
};

function ensureApprovalQueueSheet_(ss) {
  var spreadsheet = ss || getDatabaseSpreadsheet_();
  var sheet = ensureSheetWithHeadersSafely_(spreadsheet, APPROVAL_QUEUE_SHEET_NAME, APPROVAL_QUEUE_HEADERS, '#ede9fe', 'เตรียมชีต ApprovalQueue');
  setPlainTextColumnFormat_(sheet, [1,2,3,4,5,6,7,10,11,12,14]);
  if (sheet.getFrozenRows() < 1) sheet.setFrozenRows(1);
  return sheet;
}

function ensureInternalNotesSheet_(ss) {
  var spreadsheet = ss || getDatabaseSpreadsheet_();
  var sheet = ensureSheetWithHeadersSafely_(spreadsheet, INTERNAL_NOTES_SHEET_NAME, INTERNAL_NOTES_HEADERS, '#dcfce7', 'เตรียมชีต InternalNotes');
  setPlainTextColumnFormat_(sheet, [1,2,3,5,6,7,8,9,10]);
  if (sheet.getFrozenRows() < 1) sheet.setFrozenRows(1);
  return sheet;
}

function normalizeApprovalActionKey_(value) {
  return String(value || '').trim().toLowerCase();
}

function getApprovalActionDefinition_(actionKey) {
  var normalized = normalizeApprovalActionKey_(actionKey);
  return APPROVAL_ACTION_CATALOG[normalized] || null;
}

function sanitizeApprovalReason_(reason) {
  return String(reason || '').replace(/\s+/g, ' ').trim().substring(0, 250);
}

function getCurrentActorSnapshot_() {
  var sessionData = requireAuthenticatedSession_();
  var username = String(sessionData.username || '').trim();
  var fullName = String(sessionData.fullName || username || '').trim() || username;
  var role = String(sessionData.role || '').trim() || 'staff';
  return { username: username, fullName: fullName, role: role };
}

function parseJsonSafe_(value, fallback) {
  try {
    if (value === null || value === undefined || value === '') return fallback;
    if (typeof value === 'string') return JSON.parse(value);
    return value;
  } catch (e) {
    return fallback;
  }
}

function normalizeApprovalPayload_(actionKey, payload) {
  var normalizedAction = normalizeApprovalActionKey_(actionKey);
  var raw = (typeof payload === 'object' && payload !== null) ? payload : {};
  if (normalizedAction === 'reverse_payment') {
    var transactionId = String(raw.transactionId || raw.id || '').trim();
    if (!transactionId) throw new Error('กรุณาระบุรายการรับชำระที่ต้องการส่งขออนุมัติกลับรายการ');
    return { transactionId: transactionId, source: String(raw.source || '').trim(), note: String(raw.note || '').trim() };
  }
  if (normalizedAction === 'close_accounting') {
    return { monthKey: String(raw.monthKey || '').trim(), note: String(raw.note || '').trim() };
  }
  if (normalizedAction === 'backup_transactions') {
    return { monthKey: String(raw.monthKey || '').trim(), note: String(raw.note || '').trim() };
  }
  throw new Error('ไม่รองรับ action ที่ต้องการส่งขออนุมัติ');
}

function createApprovalRequestRow_(approvalId, actor, actionKey, payload, reason) {
  var definition = getApprovalActionDefinition_(actionKey);
  var entityType = definition ? definition.entityType : 'SYSTEM';
  var entityId = '-';
  if (actionKey === 'reverse_payment') entityId = String(payload.transactionId || '').trim() || '-';
  else if (payload && payload.monthKey) entityId = String(payload.monthKey || '').trim() || '-';
  return [
    approvalId,
    getThaiDate(new Date()).dateTime,
    String(actor.username || '').trim(),
    String(actor.fullName || actor.username || '').trim(),
    actionKey,
    entityType,
    entityId,
    JSON.stringify(payload || {}),
    sanitizeApprovalReason_(reason),
    'Pending',
    '',
    '',
    '',
    '',
    ''
  ];
}

function findApprovalRequestRowById_(sheet, requestId) {
  if (!sheet || !requestId || sheet.getLastRow() <= 1) return null;
  var values = sheet.getRange(2, 1, sheet.getLastRow() - 1, APPROVAL_QUEUE_HEADERS.length).getValues();
  for (var i = 0; i < values.length; i++) {
    if (String(values[i][0] || '').trim() === String(requestId || '').trim()) {
      return { rowIndex: i + 2, values: values[i] };
    }
  }
  return null;
}

function getApprovalRequestRecordFromValues_(row) {
  var safe = (row || []).slice();
  while (safe.length < APPROVAL_QUEUE_HEADERS.length) safe.push('');
  return {
    requestId: String(safe[0] || '').trim(),
    requestedAt: String(safe[1] || '').trim(),
    requestedBy: String(safe[2] || '').trim(),
    requestedByName: String(safe[3] || '').trim(),
    actionKey: String(safe[4] || '').trim(),
    entityType: String(safe[5] || '').trim(),
    entityId: String(safe[6] || '').trim(),
    payload: parseJsonSafe_(safe[7], {}),
    payloadJson: String(safe[7] || '').trim(),
    reason: String(safe[8] || '').trim(),
    status: String(safe[9] || '').trim() || 'Pending',
    decisionBy: String(safe[10] || '').trim(),
    decisionAt: String(safe[11] || '').trim(),
    decisionNote: String(safe[12] || '').trim(),
    resultRef: String(safe[13] || '').trim(),
    resultJson: String(safe[14] || '').trim(),
    actionLabel: (getApprovalActionDefinition_(safe[4]) || {}).label || String(safe[4] || '').trim()
  };
}

function updateApprovalRequestDecision_(sheet, rowIndex, updates) {
  if (!sheet || !rowIndex || rowIndex <= 1) return;
  sheet.getRange(rowIndex, 10, 1, 6).setValues([[
    String(updates.status || '').trim(),
    String(updates.decisionBy || '').trim(),
    String(updates.decisionAt || '').trim(),
    String(updates.decisionNote || '').trim(),
    String(updates.resultRef || '').trim(),
    String(updates.resultJson || '').trim()
  ]]);
}

function listApprovalRequests(filterStr) {
  requirePermission_('approvals.view', 'ไม่มีสิทธิ์ดูคิวคำขออนุมัติ');
  try {
    var filter = typeof filterStr === 'string' ? JSON.parse(filterStr || '{}') : (filterStr || {});
    var statusFilter = String(filter.status || 'all').trim().toLowerCase();
    var limit = Math.max(1, Math.min(500, Number(filter.limit) || 150));
    var cacheKey = 'approvalRequests::' + statusFilter + '::' + limit;
    var cached = getJsonCache_(cacheKey);
    if (cached && Array.isArray(cached.records)) {
      return { status: 'Success', records: cached.records.slice(0, limit) };
    }
    var ss = getDatabaseSpreadsheet_();
    var sheet = ensureApprovalQueueSheet_(ss);
    if (sheet.getLastRow() <= 1) return { status: 'Success', records: [] };
    var values = sheet.getRange(2, 1, sheet.getLastRow() - 1, APPROVAL_QUEUE_HEADERS.length).getValues();
    var records = values.map(getApprovalRequestRecordFromValues_).filter(function(record) {
      if (statusFilter === 'all') return true;
      return String(record.status || '').toLowerCase() === statusFilter;
    });
    records.sort(function(a, b) {
      var aPending = String(a.status || '').toLowerCase() === 'pending' ? 0 : 1;
      var bPending = String(b.status || '').toLowerCase() === 'pending' ? 0 : 1;
      if (aPending !== bPending) return aPending - bPending;
      var dateA = getThaiDateNumber_(a.requestedAt);
      var dateB = getThaiDateNumber_(b.requestedAt);
      if (dateA !== dateB) return dateB - dateA;
      return String(b.requestedAt || '').localeCompare(String(a.requestedAt || ''));
    });
    var payload = { status: 'Success', records: records.slice(0, limit) };
    putJsonCache_(cacheKey, payload, 15);
    return payload;
  } catch (e) {
    return { status: 'Error', message: 'โหลดคิวคำขออนุมัติไม่สำเร็จ: ' + e.toString(), records: [] };
  }
}

function submitApprovalRequest(payloadStr) {
  requirePermission_('approvals.request', 'ไม่มีสิทธิ์ส่งคำขออนุมัติ');
  var lock = LockService.getScriptLock();
  var locked = false;
  try {
    lock.waitLock(15000);
    locked = true;
    var payload = typeof payloadStr === 'string' ? JSON.parse(payloadStr || '{}') : (payloadStr || {});
    var actionKey = normalizeApprovalActionKey_(payload.actionKey);
    var definition = getApprovalActionDefinition_(actionKey);
    if (!definition) return { status: 'Error', message: 'action ที่ขออนุมัติยังไม่รองรับในระบบ' };
    var normalizedPayload = normalizeApprovalPayload_(actionKey, payload.payload || payload);
    var reason = sanitizeApprovalReason_(payload.reason || normalizedPayload.note || '');
    if (!reason) return { status: 'Error', message: 'กรุณาระบุเหตุผลของคำขออนุมัติ' };

    var ss = getDatabaseSpreadsheet_();
    var sheet = ensureApprovalQueueSheet_(ss);
    var actor = getCurrentActorSnapshot_();
    var approvalId = generateDocumentReference_('APR', new Date(), ss);
    sheet.appendRow(createApprovalRequestRow_(approvalId, actor, actionKey, normalizedPayload, reason));

    appendAuditLogSafe_(ss, {
      action: 'SUBMIT_APPROVAL_REQUEST',
      entityType: 'APPROVAL_REQUEST',
      entityId: approvalId,
      referenceNo: approvalId,
      after: { actionKey: actionKey, payload: normalizedPayload, status: 'Pending' },
      reason: reason,
      details: 'ส่งคำขออนุมัติ: ' + String(definition.label || actionKey)
    });
    logSystemActionFast(ss, 'ส่งคำขออนุมัติ', approvalId + ' | ' + definition.label, actor.username || 'system');
    clearAppCache_();
    return { status: 'Success', requestId: approvalId, actionKey: actionKey, actionLabel: definition.label, message: 'ส่งคำขออนุมัติเรียบร้อยแล้ว' };
  } catch (e) {
    return { status: 'Error', message: 'ส่งคำขออนุมัติไม่สำเร็จ: ' + e.toString() };
  } finally {
    if (locked) lock.releaseLock();
  }
}

function executeApprovalActionByRecord_(record) {
  var actionKey = normalizeApprovalActionKey_(record && record.actionKey);
  var payload = record && record.payload ? record.payload : {};
  if (actionKey === 'reverse_payment') {
    return parseJsonSafe_(cancelPayment(String(payload.transactionId || '').trim(), String(record.reason || '').trim()), { status: 'Error', message: 'กลับรายการไม่สำเร็จ' });
  }
  if (actionKey === 'close_accounting') return closeAccountingPeriod();
  if (actionKey === 'backup_transactions') return backupTransactionsToMonthlySheets();
  throw new Error('ยังไม่รองรับ action ที่ต้องการอนุมัติ');
}

function processApprovalRequest(requestId, decision, note) {
  requirePermission_('approvals.manage', 'ไม่มีสิทธิ์อนุมัติคำขอ');
  var lock = LockService.getScriptLock();
  var locked = false;
  try {
    lock.waitLock(30000);
    locked = true;
    var normalizedDecision = String(decision || '').trim().toLowerCase();
    if (normalizedDecision !== 'approve' && normalizedDecision !== 'reject') return { status: 'Error', message: 'กรุณาระบุผลการตัดสิน approve หรือ reject' };
    var normalizedRequestId = String(requestId || '').trim();
    if (!normalizedRequestId) return { status: 'Error', message: 'ไม่พบคำขอที่ต้องการดำเนินการ' };

    var ss = getDatabaseSpreadsheet_();
    var sheet = ensureApprovalQueueSheet_(ss);
    var matched = findApprovalRequestRowById_(sheet, normalizedRequestId);
    if (!matched) return { status: 'Error', message: 'ไม่พบคำขออนุมัติที่เลือก' };
    var record = getApprovalRequestRecordFromValues_(matched.values);
    if (String(record.status || '').toLowerCase() !== 'pending') return { status: 'Error', message: 'คำขอนี้ถูกดำเนินการไปแล้ว' };
    var actionKey = normalizeApprovalActionKey_(record.actionKey);

    var actor = getCurrentActorSnapshot_();
    var nowText = getThaiDate(new Date()).dateTime;
    var decisionNote = String(note || '').replace(/\s+/g, ' ').trim().substring(0, 250);

    if (normalizedDecision === 'reject') {
      updateApprovalRequestDecision_(sheet, matched.rowIndex, { status: 'Rejected', decisionBy: actor.username, decisionAt: nowText, decisionNote: decisionNote });
      appendAuditLogSafe_(ss, { action: 'REJECT_APPROVAL_REQUEST', entityType: 'APPROVAL_REQUEST', entityId: normalizedRequestId, referenceNo: normalizedRequestId, before: record, after: { status: 'Rejected', decisionBy: actor.username, decisionAt: nowText, decisionNote: decisionNote }, reason: decisionNote, details: 'ปฏิเสธคำขออนุมัติ ' + String(record.actionLabel || record.actionKey || '') });
      clearAppCache_();
      return { status: 'Success', requestId: normalizedRequestId, requestStatus: 'Rejected', actionKey: actionKey, message: 'ปฏิเสธคำขอเรียบร้อยแล้ว' };
    }

    var executionResult = executeApprovalActionByRecord_(record);
    if (!executionResult || executionResult.status === 'Error') {
      updateApprovalRequestDecision_(sheet, matched.rowIndex, { status: 'Failed', decisionBy: actor.username, decisionAt: nowText, decisionNote: decisionNote, resultJson: serializeAuditValue_(executionResult || {}) });
      return { status: 'Error', message: (executionResult && executionResult.message) || 'ไม่สามารถดำเนินการตามคำขอได้' };
    }

    var resultRef = String(executionResult.reversalTransactionId || executionResult.closedMonthKey || executionResult.archiveSheetName || '').trim();
    updateApprovalRequestDecision_(sheet, matched.rowIndex, { status: 'Approved', decisionBy: actor.username, decisionAt: nowText, decisionNote: decisionNote, resultRef: resultRef, resultJson: serializeAuditValue_(executionResult) });
    appendAuditLogSafe_(ss, { action: 'APPROVE_APPROVAL_REQUEST', entityType: 'APPROVAL_REQUEST', entityId: normalizedRequestId, referenceNo: normalizedRequestId, before: record, after: { status: 'Approved', decisionBy: actor.username, decisionAt: nowText, resultRef: resultRef, executionResult: executionResult }, reason: decisionNote, details: 'อนุมัติคำขอ ' + String(record.actionLabel || record.actionKey || '') });
    clearAppCache_();
    return { status: 'Success', requestId: normalizedRequestId, requestStatus: 'Approved', actionKey: actionKey, result: executionResult, message: 'อนุมัติคำขอเรียบร้อยแล้ว' };
  } catch (e) {
    return { status: 'Error', message: 'ดำเนินการคำขออนุมัติไม่สำเร็จ: ' + e.toString() };
  } finally {
    if (locked) lock.releaseLock();
  }
}

function listInternalNotesByEntity_(ss, entityType, entityId) {
  var spreadsheet = ss || getDatabaseSpreadsheet_();
  var sheet = ensureInternalNotesSheet_(spreadsheet);
  var normalizedType = String(entityType || '').trim().toUpperCase();
  var normalizedId = String(entityId || '').trim();
  if (!normalizedType || !normalizedId || sheet.getLastRow() <= 1) return [];
  var values = sheet.getRange(2, 1, sheet.getLastRow() - 1, INTERNAL_NOTES_HEADERS.length).getValues();
  var records = [];
  for (var i = 0; i < values.length; i++) {
    var row = values[i];
    if (String(row[1] || '').trim().toUpperCase() !== normalizedType) continue;
    if (String(row[2] || '').trim() !== normalizedId) continue;
    if (String(row[4] || '').trim().toLowerCase() === 'deleted') continue;
    records.push({
      noteId: String(row[0] || '').trim(),
      entityType: String(row[1] || '').trim(),
      entityId: String(row[2] || '').trim(),
      noteText: String(row[3] || '').trim(),
      status: String(row[4] || '').trim() || 'Active',
      createdBy: String(row[5] || '').trim(),
      createdByName: String(row[6] || '').trim(),
      createdAt: String(row[7] || '').trim(),
      updatedBy: String(row[8] || '').trim(),
      updatedAt: String(row[9] || '').trim()
    });
  }
  records.sort(function(a, b) {
    var dateA = getThaiDateNumber_(a.createdAt);
    var dateB = getThaiDateNumber_(b.createdAt);
    if (dateA !== dateB) return dateB - dateA;
    return String(b.createdAt || '').localeCompare(String(a.createdAt || ''));
  });
  return records;
}

function getInternalNotes(entityType, entityId) {
  requirePermission_('notes.view', 'ไม่มีสิทธิ์ดูหมายเหตุภายใน');
  try {
    var ss = getDatabaseSpreadsheet_();
    return { status: 'Success', notes: listInternalNotesByEntity_(ss, entityType, entityId) };
  } catch (e) {
    return { status: 'Error', message: 'โหลดหมายเหตุภายในไม่สำเร็จ: ' + e.toString(), notes: [] };
  }
}

function saveInternalNote(payloadStr) {
  requirePermission_('notes.manage', 'ไม่มีสิทธิ์บันทึกหมายเหตุภายใน');
  var lock = LockService.getScriptLock();
  var locked = false;
  try {
    lock.waitLock(10000);
    locked = true;
    var payload = typeof payloadStr === 'string' ? JSON.parse(payloadStr || '{}') : (payloadStr || {});
    var entityType = String(payload.entityType || '').trim().toUpperCase();
    var entityId = String(payload.entityId || '').trim();
    var noteText = String(payload.noteText || '').trim();
    if (!entityType || !entityId) return { status: 'Error', message: 'กรุณาระบุข้อมูลอ้างอิงของหมายเหตุ' };
    if (!noteText) return { status: 'Error', message: 'กรุณากรอกข้อความหมายเหตุ' };
    var actor = getCurrentActorSnapshot_();
    var ss = getDatabaseSpreadsheet_();
    var sheet = ensureInternalNotesSheet_(ss);
    var noteId = generateDocumentReference_('NOTE', new Date(), ss);
    var nowText = getThaiDate(new Date()).dateTime;
    sheet.appendRow([noteId, entityType, entityId, noteText.substring(0, 1000), 'Active', actor.username, actor.fullName, nowText, actor.username, nowText]);
    appendAuditLogSafe_(ss, { action: 'ADD_INTERNAL_NOTE', entityType: entityType, entityId: entityId, referenceNo: noteId, after: { noteId: noteId, noteText: noteText.substring(0, 1000) }, details: 'บันทึกหมายเหตุภายใน' });
    return { status: 'Success', noteId: noteId, notes: listInternalNotesByEntity_(ss, entityType, entityId), message: 'บันทึกหมายเหตุเรียบร้อยแล้ว' };
  } catch (e) {
    return { status: 'Error', message: 'บันทึกหมายเหตุไม่สำเร็จ: ' + e.toString() };
  } finally {
    if (locked) lock.releaseLock();
  }
}

function deleteInternalNote(noteId) {
  requirePermission_('notes.manage', 'ไม่มีสิทธิ์ลบหมายเหตุภายใน');
  var lock = LockService.getScriptLock();
  var locked = false;
  try {
    lock.waitLock(10000);
    locked = true;
    var normalizedId = String(noteId || '').trim();
    if (!normalizedId) return { status: 'Error', message: 'ไม่พบหมายเหตุที่ต้องการลบ' };
    var ss = getDatabaseSpreadsheet_();
    var sheet = ensureInternalNotesSheet_(ss);
    if (sheet.getLastRow() <= 1) return { status: 'Error', message: 'ไม่พบหมายเหตุที่ต้องการลบ' };
    var values = sheet.getRange(2, 1, sheet.getLastRow() - 1, INTERNAL_NOTES_HEADERS.length).getValues();
    var actor = getCurrentActorSnapshot_();
    for (var i = 0; i < values.length; i++) {
      if (String(values[i][0] || '').trim() !== normalizedId) continue;
      var rowIndex = i + 2;
      var entityType = String(values[i][1] || '').trim();
      var entityId = String(values[i][2] || '').trim();
      sheet.getRange(rowIndex, 5, 1, 6).setValues([['Deleted', String(values[i][5] || '').trim(), String(values[i][6] || '').trim(), String(values[i][7] || '').trim(), actor.username, getThaiDate(new Date()).dateTime]]);
      appendAuditLogSafe_(ss, { action: 'DELETE_INTERNAL_NOTE', entityType: entityType, entityId: entityId, referenceNo: normalizedId, before: { noteId: normalizedId, noteText: String(values[i][3] || '').trim() }, after: { status: 'Deleted' }, details: 'ลบหมายเหตุภายใน' });
      return { status: 'Success', notes: listInternalNotesByEntity_(ss, entityType, entityId), message: 'ลบหมายเหตุเรียบร้อยแล้ว' };
    }
    return { status: 'Error', message: 'ไม่พบหมายเหตุที่ต้องการลบ' };
  } catch (e) {
    return { status: 'Error', message: 'ลบหมายเหตุไม่สำเร็จ: ' + e.toString() };
  } finally {
    if (locked) lock.releaseLock();
  }
}

var __phase5_orig_getMemberDetail = getMemberDetail;
getMemberDetail = function(memberId) {
  var result = __phase5_orig_getMemberDetail(memberId);
  if (!result || result.status === 'Error') return result;
  try {
    var ss = getDatabaseSpreadsheet_();
    var canViewNotes = true;
    try { requirePermission_('notes.view', 'ไม่มีสิทธิ์ดูหมายเหตุภายใน'); } catch (e) { canViewNotes = false; }
    result.notes = canViewNotes ? listInternalNotesByEntity_(ss, 'MEMBER', String((result.member || {}).id || '').trim()) : [];
  } catch (e) {
    result.notes = [];
  }
  return result;
};

var __phase5_orig_getLoanDetail = getLoanDetail;
getLoanDetail = function(contractNo) {
  var result = __phase5_orig_getLoanDetail(contractNo);
  if (!result || result.status === 'Error') return result;
  try {
    var ss = getDatabaseSpreadsheet_();
    var canViewNotes = true;
    try { requirePermission_('notes.view', 'ไม่มีสิทธิ์ดูหมายเหตุภายใน'); } catch (e) { canViewNotes = false; }
    result.notes = canViewNotes ? listInternalNotesByEntity_(ss, 'LOAN', String((result.loan || {}).contract || '').trim()) : [];
  } catch (e) {
    result.notes = [];
  }
  return result;
};


// ==========================================
// PHASE 6: ATTACHMENTS + NOTIFICATIONS + APPROVAL DASHBOARD
// ==========================================
var ATTACHMENTS_INDEX_SHEET_NAME = 'AttachmentsIndex';
var ATTACHMENTS_INDEX_HEADERS = [
  'AttachmentId', 'EntityType', 'EntityId', 'FileName', 'MimeType', 'FileId', 'FileUrl', 'Note', 'Status',
  'CreatedBy', 'CreatedByName', 'CreatedAt', 'UpdatedBy', 'UpdatedAt'
];

(function extendPhase6Permissions_() {
  if (typeof PERMISSION_CATALOG === 'undefined' || !PERMISSION_CATALOG || !PERMISSION_CATALOG.push) return;
  var additionalPermissions = [
    { key: 'attachments.view', label: 'ดูไฟล์แนบของสมาชิก/สัญญา' },
    { key: 'attachments.manage', label: 'เพิ่ม/ลบไฟล์แนบของสมาชิก/สัญญา' },
    { key: 'notifications.manage', label: 'จัดการการแจ้งเตือนอีเมล' }
  ];
  for (var i = 0; i < additionalPermissions.length; i++) {
    var item = additionalPermissions[i];
    var exists = false;
    for (var j = 0; j < PERMISSION_CATALOG.length; j++) {
      if (String(PERMISSION_CATALOG[j].key || '').trim() === item.key) {
        exists = true;
        break;
      }
    }
    if (!exists) PERMISSION_CATALOG.push(item);
  }
})();

var __phase6_orig_getDefaultPermissionMapByRole_ = getDefaultPermissionMapByRole_;
getDefaultPermissionMapByRole_ = function(role) {
  var map = __phase6_orig_getDefaultPermissionMapByRole_(role);
  var normalizedRole = normalizeUserRole_(role);
  if (normalizedRole === 'admin') return map;
  map['attachments.view'] = true;
  map['attachments.manage'] = true;
  if (!Object.prototype.hasOwnProperty.call(map, 'notifications.manage')) map['notifications.manage'] = false;
  return map;
};

function ensureAttachmentsIndexSheet_(ss) {
  var spreadsheet = ss || getDatabaseSpreadsheet_();
  var sheet = ensureSheetWithHeadersSafely_(spreadsheet, ATTACHMENTS_INDEX_SHEET_NAME, ATTACHMENTS_INDEX_HEADERS, '#fef3c7', 'เตรียมชีต AttachmentsIndex');
  setPlainTextColumnFormat_(sheet, [1,2,3,4,5,6,7,8,9,10,11,12,13,14]);
  if (sheet.getFrozenRows() < 1) sheet.setFrozenRows(1);
  return sheet;
}

function normalizeAttachmentEntityType_(value) {
  var normalized = String(value || '').trim().toUpperCase();
  if (normalized === 'MEMBER' || normalized === 'LOAN') return normalized;
  return '';
}

function parseDriveFileIdFromUrl_(value) {
  var text = String(value || '').trim();
  if (!text) return '';
  var patterns = [
    /\/d\/([a-zA-Z0-9_-]{10,})/,
    /[?&]id=([a-zA-Z0-9_-]{10,})/,
    /^([a-zA-Z0-9_-]{20,})$/
  ];
  for (var i = 0; i < patterns.length; i++) {
    var match = text.match(patterns[i]);
    if (match && match[1]) return String(match[1]).trim();
  }
  return '';
}

function getAttachmentFolder_(ss) {
  var spreadsheet = ss || getDatabaseSpreadsheet_();
  var settingsSheet = spreadsheet.getSheetByName('Settings') || createSheetSafely_(spreadsheet, 'Settings', 3, 2, 'เตรียมชีต Settings');
  var folderId = String(getSettingValue_(settingsSheet, 'AttachmentFolderId') || '').trim();
  if (folderId) {
    try {
      return DriveApp.getFolderById(folderId);
    } catch (e) {}
  }
  var folderName = String(getSettingValue_(settingsSheet, 'AttachmentFolderName') || '').trim() || 'FinanceWebAppAttachments';
  var folderIterator = DriveApp.getFoldersByName(folderName);
  var folder = folderIterator.hasNext() ? folderIterator.next() : DriveApp.createFolder(folderName);
  upsertSettingValue_(settingsSheet, 'AttachmentFolderId', folder.getId());
  upsertSettingValue_(settingsSheet, 'AttachmentFolderName', folderName);
  return folder;
}

function getNotificationSettings_(ss) {
  var spreadsheet = ss || getDatabaseSpreadsheet_();
  var settingsSheet = spreadsheet.getSheetByName('Settings') || createSheetSafely_(spreadsheet, 'Settings', 3, 2, 'เตรียมชีต Settings');
  var rawRecipients = String(getSettingValue_(settingsSheet, 'NotificationRecipients') || '').trim();
  var parseBool = function(key, defaultValue) {
    var raw = String(getSettingValue_(settingsSheet, key) || '').trim().toLowerCase();
    if (!raw) return !!defaultValue;
    return raw === 'true' || raw === '1' || raw === 'yes';
  };
  return {
    recipients: rawRecipients,
    notifyApprovalSubmitted: parseBool('NotifyApprovalSubmitted', true),
    notifyApprovalApproved: parseBool('NotifyApprovalApproved', true),
    notifyApprovalRejected: parseBool('NotifyApprovalRejected', true),
    notifyAttachmentUploaded: parseBool('NotifyAttachmentUploaded', false)
  };
}

function getNotificationSettings() {
  requirePermission_('notifications.manage', 'ไม่มีสิทธิ์ดูการตั้งค่าการแจ้งเตือน');
  try {
    var cacheKey = 'notificationSettingsCache';
    var cached = getJsonCache_(cacheKey);
    if (cached && cached.settings) {
      return { status: 'Success', settings: cached.settings, cached: true };
    }
    var settings = getNotificationSettings_(getDatabaseSpreadsheet_());
    putJsonCache_(cacheKey, { settings: settings }, 60);
    return { status: 'Success', settings: settings, cached: false };
  } catch (e) {
    return { status: 'Error', message: 'โหลดการตั้งค่าการแจ้งเตือนไม่สำเร็จ: ' + e.toString(), settings: getNotificationSettings_(getDatabaseSpreadsheet_()) };
  }
}

function sendNotificationEmailIfEnabled_(ss, flagKey, subject, lines) {
  try {
    var settings = getNotificationSettings_(ss);
    if (!settings || !settings[flagKey]) return;
    var recipients = String(settings.recipients || '').trim();
    if (!recipients) return;
    var cleanRecipients = recipients.split(/[\n,;]+/).map(function(item) { return String(item || '').trim(); }).filter(function(item) { return !!item; }).join(',');
    if (!cleanRecipients) return;
    var body = Array.isArray(lines) ? lines.join('\n') : String(lines || '');
    MailApp.sendEmail({
      to: cleanRecipients,
      subject: String(subject || 'แจ้งเตือนระบบการเงิน'),
      body: body,
      name: 'Finance Web App'
    });
  } catch (e) {
    console.error('sendNotificationEmailIfEnabled_ failed:', e);
  }
}

function getApprovalDashboardSummary(filterStr) {
  requirePermission_('approvals.view', 'ไม่มีสิทธิ์ดูแดชบอร์ดคำขออนุมัติ');
  try {
    var filter = typeof filterStr === 'string' ? JSON.parse(filterStr || '{}') : (filterStr || {});
    var days = Math.max(1, Math.min(3650, Number(filter.days) || 365));
    var ss = getDatabaseSpreadsheet_();
    var sheet = ensureApprovalQueueSheet_(ss);
    if (sheet.getLastRow() <= 1) {
      return { status: 'Success', summary: { total: 0, pending: 0, approved: 0, rejected: 0, failed: 0 }, actions: [], recent: [] };
    }
    var values = sheet.getRange(2, 1, sheet.getLastRow() - 1, APPROVAL_QUEUE_HEADERS.length).getValues();
    var cutoff = new Date();
    cutoff.setDate(cutoff.getDate() - days);
    var cutoffNumber = cutoff.getFullYear() * 10000 + (cutoff.getMonth() + 1) * 100 + cutoff.getDate();
    var summary = { total: 0, pending: 0, approved: 0, rejected: 0, failed: 0 };
    var actionMap = {};
    var recent = [];
    for (var i = 0; i < values.length; i++) {
      var record = getApprovalRequestRecordFromValues_(values[i]);
      var requestedDateNo = getThaiDateNumber_(record.requestedAt);
      if (requestedDateNo && requestedDateNo < cutoffNumber) continue;
      summary.total++;
      var statusKey = String(record.status || '').trim().toLowerCase();
      if (statusKey === 'approved') summary.approved++;
      else if (statusKey === 'rejected') summary.rejected++;
      else if (statusKey === 'failed') summary.failed++;
      else summary.pending++;
      var actionKey = String(record.actionKey || '').trim() || 'unknown';
      if (!actionMap[actionKey]) actionMap[actionKey] = { actionKey: actionKey, actionLabel: String(record.actionLabel || actionKey), total: 0, pending: 0, approved: 0, rejected: 0, failed: 0 };
      actionMap[actionKey].total++;
      if (statusKey === 'approved') actionMap[actionKey].approved++;
      else if (statusKey === 'rejected') actionMap[actionKey].rejected++;
      else if (statusKey === 'failed') actionMap[actionKey].failed++;
      else actionMap[actionKey].pending++;
      recent.push({
        requestId: record.requestId,
        actionLabel: record.actionLabel,
        status: record.status,
        requestedByName: record.requestedByName || record.requestedBy,
        requestedAt: record.requestedAt,
        decisionAt: record.decisionAt,
        decisionBy: record.decisionBy,
        reason: record.reason,
        resultRef: record.resultRef
      });
    }
    var actions = Object.keys(actionMap).map(function(key) { return actionMap[key]; }).sort(function(a, b) { return b.total - a.total; });
    recent.sort(function(a, b) { return String(b.requestedAt || '').localeCompare(String(a.requestedAt || '')); });
    return { status: 'Success', summary: summary, actions: actions, recent: recent.slice(0, 12) };
  } catch (e) {
    return { status: 'Error', message: 'โหลดแดชบอร์ดคำขออนุมัติไม่สำเร็จ: ' + e.toString(), summary: null, actions: [], recent: [] };
  }
}

function listAttachmentsByEntity_(ss, entityType, entityId) {
  var spreadsheet = ss || getDatabaseSpreadsheet_();
  var sheet = ensureAttachmentsIndexSheet_(spreadsheet);
  var normalizedType = normalizeAttachmentEntityType_(entityType);
  var normalizedId = String(entityId || '').trim();
  if (!normalizedType || !normalizedId || sheet.getLastRow() <= 1) return [];
  var values = sheet.getRange(2, 1, sheet.getLastRow() - 1, ATTACHMENTS_INDEX_HEADERS.length).getValues();
  var records = [];
  for (var i = 0; i < values.length; i++) {
    var row = values[i];
    if (String(row[1] || '').trim().toUpperCase() !== normalizedType) continue;
    if (String(row[2] || '').trim() !== normalizedId) continue;
    if (String(row[8] || '').trim().toLowerCase() === 'deleted') continue;
    records.push({
      attachmentId: String(row[0] || '').trim(),
      entityType: String(row[1] || '').trim(),
      entityId: String(row[2] || '').trim(),
      fileName: String(row[3] || '').trim(),
      mimeType: String(row[4] || '').trim(),
      fileId: String(row[5] || '').trim(),
      fileUrl: String(row[6] || '').trim(),
      note: String(row[7] || '').trim(),
      status: String(row[8] || '').trim() || 'Active',
      createdBy: String(row[9] || '').trim(),
      createdByName: String(row[10] || '').trim(),
      createdAt: String(row[11] || '').trim(),
      updatedBy: String(row[12] || '').trim(),
      updatedAt: String(row[13] || '').trim()
    });
  }
  records.sort(function(a, b) { return String(b.createdAt || '').localeCompare(String(a.createdAt || '')); });
  return records;
}

function saveAttachmentFile(payloadStr) {
  requirePermission_('attachments.manage', 'ไม่มีสิทธิ์อัปโหลดไฟล์แนบ');
  var lock = LockService.getScriptLock();
  var locked = false;
  try {
    lock.waitLock(30000);
    locked = true;
    var payload = typeof payloadStr === 'string' ? JSON.parse(payloadStr || '{}') : (payloadStr || {});
    var entityType = normalizeAttachmentEntityType_(payload.entityType);
    var entityId = String(payload.entityId || '').trim();
    var fileName = String(payload.fileName || '').trim();
    var mimeType = String(payload.mimeType || 'application/octet-stream').trim();
    var base64Data = String(payload.base64Data || '').trim();
    var note = String(payload.note || '').trim().substring(0, 500);
    if (!entityType || !entityId) return { status: 'Error', message: 'กรุณาระบุข้อมูลสมาชิกหรือสัญญาที่ต้องการแนบไฟล์' };
    if (!fileName || !base64Data) return { status: 'Error', message: 'กรุณาเลือกไฟล์ที่ต้องการอัปโหลด' };

    var ss = getDatabaseSpreadsheet_();
    var folder = getAttachmentFolder_(ss);
    var actor = getCurrentActorSnapshot_();
    var nowText = getThaiDate(new Date()).dateTime;
    var attachmentId = generateDocumentReference_('ATT', new Date(), ss);
    var safeFileName = fileName.replace(/[\\/:*?"<>|]/g, '_');
    var blob = Utilities.newBlob(Utilities.base64Decode(base64Data), mimeType, attachmentId + '_' + safeFileName);
    var file = folder.createFile(blob);
    try { file.setDescription('Attachment ' + attachmentId + ' | ' + entityType + ' | ' + entityId); } catch (e) {}
    var fileUrl = 'https://drive.google.com/file/d/' + file.getId() + '/view';

    var sheet = ensureAttachmentsIndexSheet_(ss);
    sheet.appendRow([attachmentId, entityType, entityId, safeFileName, mimeType, file.getId(), fileUrl, note, 'Active', actor.username, actor.fullName, nowText, actor.username, nowText]);
    appendAuditLogSafe_(ss, {
      action: 'ADD_ATTACHMENT',
      entityType: entityType,
      entityId: entityId,
      referenceNo: attachmentId,
      after: { attachmentId: attachmentId, fileName: safeFileName, fileId: file.getId(), note: note },
      details: 'เพิ่มไฟล์แนบให้ ' + entityType + ' ' + entityId
    });
    sendNotificationEmailIfEnabled_(ss, 'notifyAttachmentUploaded', 'แจ้งเตือน: มีการอัปโหลดไฟล์แนบใหม่', [
      'มีการอัปโหลดไฟล์แนบใหม่ในระบบ',
      'เลขอ้างอิงไฟล์แนบ: ' + attachmentId,
      'ประเภท: ' + entityType,
      'รหัสอ้างอิง: ' + entityId,
      'ไฟล์: ' + safeFileName,
      'ผู้อัปโหลด: ' + (actor.fullName || actor.username),
      'เวลา: ' + nowText,
      note ? ('หมายเหตุ: ' + note) : ''
    ].filter(function(line) { return !!line; }));
    clearAppCache_();
    return { status: 'Success', attachmentId: attachmentId, attachments: listAttachmentsByEntity_(ss, entityType, entityId), message: 'อัปโหลดไฟล์แนบเรียบร้อยแล้ว' };
  } catch (e) {
    return { status: 'Error', message: 'อัปโหลดไฟล์แนบไม่สำเร็จ: ' + e.toString() };
  } finally {
    if (locked) lock.releaseLock();
  }
}

function deleteAttachment(attachmentId) {
  requirePermission_('attachments.manage', 'ไม่มีสิทธิ์ลบไฟล์แนบ');
  var lock = LockService.getScriptLock();
  var locked = false;
  try {
    lock.waitLock(30000);
    locked = true;
    var normalizedId = String(attachmentId || '').trim();
    if (!normalizedId) return { status: 'Error', message: 'ไม่พบไฟล์แนบที่ต้องการลบ' };
    var ss = getDatabaseSpreadsheet_();
    var sheet = ensureAttachmentsIndexSheet_(ss);
    if (sheet.getLastRow() <= 1) return { status: 'Error', message: 'ไม่พบไฟล์แนบในระบบ' };
    var values = sheet.getRange(2, 1, sheet.getLastRow() - 1, ATTACHMENTS_INDEX_HEADERS.length).getValues();
    var actor = getCurrentActorSnapshot_();
    var nowText = getThaiDate(new Date()).dateTime;
    for (var i = 0; i < values.length; i++) {
      if (String(values[i][0] || '').trim() !== normalizedId) continue;
      var entityType = String(values[i][1] || '').trim();
      var entityId = String(values[i][2] || '').trim();
      if (String(values[i][8] || '').trim().toLowerCase() === 'deleted') {
        return { status: 'Error', message: 'ไฟล์แนบนี้ถูกลบออกจากสารบัญแล้ว' };
      }
      sheet.getRange(i + 2, 9, 1, 6).setValues([[
        'Deleted',
        String(values[i][9] || '').trim(),
        String(values[i][10] || '').trim(),
        String(values[i][11] || '').trim(),
        actor.username,
        nowText
      ]]);
      appendAuditLogSafe_(ss, {
        action: 'DELETE_ATTACHMENT',
        entityType: entityType,
        entityId: entityId,
        referenceNo: normalizedId,
        before: { attachmentId: normalizedId, fileName: String(values[i][3] || '').trim(), fileId: String(values[i][5] || '').trim() },
        after: { status: 'Deleted' },
        details: 'ลบไฟล์แนบออกจากสารบัญ'
      });
      clearAppCache_();
      return { status: 'Success', attachments: listAttachmentsByEntity_(ss, entityType, entityId), message: 'ลบไฟล์แนบออกจากสารบัญเรียบร้อยแล้ว' };
    }
    return { status: 'Error', message: 'ไม่พบไฟล์แนบที่เลือก' };
  } catch (e) {
    return { status: 'Error', message: 'ลบไฟล์แนบไม่สำเร็จ: ' + e.toString() };
  } finally {
    if (locked) lock.releaseLock();
  }
}

var __phase6_orig_submitApprovalRequest = submitApprovalRequest;
submitApprovalRequest = function(payloadStr) {
  var result = __phase6_orig_submitApprovalRequest(payloadStr);
  try {
    if (result && result.status === 'Success') {
      var payload = typeof payloadStr === 'string' ? JSON.parse(payloadStr || '{}') : (payloadStr || {});
      var actor = getCurrentActorSnapshot_();
      var ss = getDatabaseSpreadsheet_();
      sendNotificationEmailIfEnabled_(ss, 'notifyApprovalSubmitted', 'แจ้งเตือน: มีคำขออนุมัติใหม่ในระบบ', [
        'มีคำขออนุมัติใหม่เข้าระบบ',
        'เลขคำขอ: ' + String(result.requestId || '-'),
        'รายการ: ' + String(result.actionLabel || result.actionKey || '-'),
        'ผู้ขอ: ' + (actor.fullName || actor.username),
        'เหตุผล: ' + String((payload && payload.reason) || '-')
      ]);
    }
  } catch (e) {
    console.error('submitApprovalRequest notification failed:', e);
  }
  return result;
};

var __phase6_orig_processApprovalRequest = processApprovalRequest;
processApprovalRequest = function(requestId, decision, note) {
  var result = __phase6_orig_processApprovalRequest(requestId, decision, note);
  try {
    if (result && result.status === 'Success') {
      var ss = getDatabaseSpreadsheet_();
      var actor = getCurrentActorSnapshot_();
      var normalizedDecision = String(decision || '').trim().toLowerCase();
      if (normalizedDecision === 'approve') {
        sendNotificationEmailIfEnabled_(ss, 'notifyApprovalApproved', 'แจ้งเตือน: คำขออนุมัติได้รับการอนุมัติ', [
          'คำขออนุมัติได้รับการอนุมัติแล้ว',
          'เลขคำขอ: ' + String(result.requestId || requestId || '-'),
          'ผู้อนุมัติ: ' + (actor.fullName || actor.username),
          'หมายเหตุ: ' + String(note || '-')
        ]);
      } else if (normalizedDecision === 'reject') {
        sendNotificationEmailIfEnabled_(ss, 'notifyApprovalRejected', 'แจ้งเตือน: คำขออนุมัติถูกปฏิเสธ', [
          'คำขออนุมัติถูกปฏิเสธ',
          'เลขคำขอ: ' + String(result.requestId || requestId || '-'),
          'ผู้พิจารณา: ' + (actor.fullName || actor.username),
          'หมายเหตุ: ' + String(note || '-')
        ]);
      }
    }
  } catch (e) {
    console.error('processApprovalRequest notification failed:', e);
  }
  return result;
};

var __phase6_orig_saveSettings = saveSettings;
saveSettings = function(settingsStr) {
  requireAdminSession_();
  try {
    var settingsData = typeof settingsStr === 'string' ? JSON.parse(settingsStr || '{}') : (settingsStr || {});
    var ss = getDatabaseSpreadsheet_();
    var settingsSheet = ss.getSheetByName('Settings') || createSheetSafely_(ss, 'Settings', 3, 2, 'เตรียมชีต Settings');
    if (settingsData.interestRate === undefined || settingsData.interestRate === null || settingsData.interestRate === '') {
      var currentInterest = Number(getSettingValue_(settingsSheet, 'InterestRate')) || 12.0;
      settingsData.interestRate = currentInterest;
    }
    var baseResult = __phase6_orig_saveSettings(JSON.stringify(settingsData));
    if (String(baseResult || '').indexOf('Error:') === 0) return baseResult;
    if (settingsData.notificationSettings) {
      var notificationSettings = settingsData.notificationSettings;
      upsertSettingValue_(settingsSheet, 'NotificationRecipients', String(notificationSettings.recipients || '').trim());
      upsertSettingValue_(settingsSheet, 'NotifyApprovalSubmitted', !!notificationSettings.notifyApprovalSubmitted);
      upsertSettingValue_(settingsSheet, 'NotifyApprovalApproved', !!notificationSettings.notifyApprovalApproved);
      upsertSettingValue_(settingsSheet, 'NotifyApprovalRejected', !!notificationSettings.notifyApprovalRejected);
      upsertSettingValue_(settingsSheet, 'NotifyAttachmentUploaded', !!notificationSettings.notifyAttachmentUploaded);
      appendAuditLogSafe_(ss, {
        action: 'UPDATE_NOTIFICATION_SETTINGS',
        entityType: 'SETTINGS',
        entityId: 'NotificationSettings',
        after: getNotificationSettings_(ss),
        details: 'อัปเดตการตั้งค่าการแจ้งเตือนอีเมล'
      });
    }
    clearAppCache_();
    return 'Success';
  } catch (e) {
    return 'Error: ' + e.toString();
  }
};

var __phase6_orig_getMemberDetail = getMemberDetail;
getMemberDetail = function(memberId) {
  var result = __phase6_orig_getMemberDetail(memberId);
  if (!result || result.status === 'Error') return result;
  try {
    var ss = getDatabaseSpreadsheet_();
    var canViewAttachments = true;
    try { requirePermission_('attachments.view', 'ไม่มีสิทธิ์ดูไฟล์แนบ'); } catch (e) { canViewAttachments = false; }
    result.attachments = canViewAttachments ? listAttachmentsByEntity_(ss, 'MEMBER', String((result.member || {}).id || '').trim()) : [];
  } catch (e) {
    result.attachments = [];
  }
  return result;
};

var __phase6_orig_getLoanDetail = getLoanDetail;
getLoanDetail = function(contractNo) {
  var result = __phase6_orig_getLoanDetail(contractNo);
  if (!result || result.status === 'Error') return result;
  try {
    var ss = getDatabaseSpreadsheet_();
    var canViewAttachments = true;
    try { requirePermission_('attachments.view', 'ไม่มีสิทธิ์ดูไฟล์แนบ'); } catch (e) { canViewAttachments = false; }
    result.attachments = canViewAttachments ? listAttachmentsByEntity_(ss, 'LOAN', String((result.loan || {}).contract || '').trim()) : [];
  } catch (e) {
    result.attachments = [];
  }
  return result;
};


// ===== Phase 7: Notification history + Executive dashboard =====
var NOTIFICATION_HISTORY_SHEET_NAME = 'NotificationHistory';
var NOTIFICATION_HISTORY_HEADERS = [
  'NotificationId',
  'FlagKey',
  'Subject',
  'Recipients',
  'Status',
  'BodyPreview',
  'RelatedRef',
  'ActorUsername',
  'ActorName',
  'CreatedAt',
  'ErrorMessage'
];

(function ensurePhase7PermissionCatalog_() {
  if (typeof PERMISSION_CATALOG === 'undefined' || !PERMISSION_CATALOG || !PERMISSION_CATALOG.push) return;
  var extras = [
    { key: 'notifications.view_history', label: 'ดูประวัติการแจ้งเตือนอีเมล' },
    { key: 'dashboard.executive', label: 'ดูแดชบอร์ดผู้บริหาร' }
  ];
  for (var i = 0; i < extras.length; i++) {
    var exists = false;
    for (var j = 0; j < PERMISSION_CATALOG.length; j++) {
      if (String(PERMISSION_CATALOG[j].key || '').trim() === extras[i].key) {
        exists = true;
        break;
      }
    }
    if (!exists) PERMISSION_CATALOG.push(extras[i]);
  }
})();

var __phase7_orig_getDefaultPermissionMapByRole_ = getDefaultPermissionMapByRole_;
getDefaultPermissionMapByRole_ = function(role) {
  var map = __phase7_orig_getDefaultPermissionMapByRole_(role) || {};
  if (!Object.prototype.hasOwnProperty.call(map, 'notifications.view_history')) map['notifications.view_history'] = false;
  if (!Object.prototype.hasOwnProperty.call(map, 'dashboard.executive')) map['dashboard.executive'] = false;
  var normalizedRole = normalizeUserRole_(role);
  if (normalizedRole === 'admin') {
    map['notifications.view_history'] = true;
    map['dashboard.executive'] = true;
  }
  return map;
};

function ensureNotificationHistorySheet_(ss) {
  var spreadsheet = ss || getDatabaseSpreadsheet_();
  var sheet = ensureSheetWithHeadersSafely_(spreadsheet, NOTIFICATION_HISTORY_SHEET_NAME, NOTIFICATION_HISTORY_HEADERS, '#e0f2fe', 'เตรียมชีต NotificationHistory');
  setPlainTextColumnFormat_(sheet, [1,2,3,4,5,6,7,8,9,10,11]);
  if (sheet.getFrozenRows() < 1) sheet.setFrozenRows(1);
  return sheet;
}

function appendNotificationHistory_(ss, payload) {
  try {
    var spreadsheet = ss || getDatabaseSpreadsheet_();
    var sheet = ensureNotificationHistorySheet_(spreadsheet);
    var actor = null;
    try { actor = getCurrentActorSnapshot_(); } catch (e) {}
    var createdAt = (payload && payload.createdAt) ? String(payload.createdAt) : getThaiDate(new Date()).dateTime;
    var notificationId = String((payload && payload.notificationId) || generateDocumentReference_('NTF', new Date(), spreadsheet) || '').trim();
    var row = [
      notificationId,
      String((payload && payload.flagKey) || '').trim(),
      String((payload && payload.subject) || '').trim(),
      String((payload && payload.recipients) || '').trim(),
      String((payload && payload.status) || 'UNKNOWN').trim(),
      String((payload && payload.bodyPreview) || '').trim().substring(0, 2000),
      String((payload && payload.relatedRef) || '').trim(),
      String((payload && payload.actorUsername) || (actor && actor.username) || '').trim(),
      String((payload && payload.actorName) || (actor && actor.fullName) || '').trim(),
      createdAt,
      String((payload && payload.errorMessage) || '').trim().substring(0, 1000)
    ];
    sheet.appendRow(row);
    return notificationId;
  } catch (e) {
    console.error('appendNotificationHistory_ failed:', e);
    return '';
  }
}

var __phase7_orig_sendNotificationEmailIfEnabled_ = sendNotificationEmailIfEnabled_;
sendNotificationEmailIfEnabled_ = function(ss, flagKey, subject, lines, meta) {
  var spreadsheet = ss || getDatabaseSpreadsheet_();
  var settings = getNotificationSettings_(spreadsheet);
  var body = Array.isArray(lines) ? lines.join('\n') : String(lines || '');
  var recipients = String((settings && settings.recipients) || '').trim();
  var cleanRecipients = recipients.split(/[\n,;]+/).map(function(item) { return String(item || '').trim(); }).filter(function(item) { return !!item; }).join(',');
  var basePayload = {
    flagKey: flagKey,
    subject: String(subject || 'แจ้งเตือนระบบการเงิน'),
    recipients: cleanRecipients,
    bodyPreview: body,
    relatedRef: String((meta && meta.relatedRef) || '').trim()
  };
  try {
    if (!settings || !settings[flagKey]) {
      appendNotificationHistory_(spreadsheet, Object.assign({}, basePayload, { status: 'SKIPPED_DISABLED' }));
      return;
    }
    if (!cleanRecipients) {
      appendNotificationHistory_(spreadsheet, Object.assign({}, basePayload, { status: 'SKIPPED_NO_RECIPIENT' }));
      return;
    }
    MailApp.sendEmail({
      to: cleanRecipients,
      subject: String(subject || 'แจ้งเตือนระบบการเงิน'),
      body: body,
      name: 'Finance Web App'
    });
    appendNotificationHistory_(spreadsheet, Object.assign({}, basePayload, { status: 'SENT' }));
  } catch (e) {
    appendNotificationHistory_(spreadsheet, Object.assign({}, basePayload, { status: 'FAILED', errorMessage: e.toString() }));
    console.error('sendNotificationEmailIfEnabled_ failed:', e);
  }
};

function getNotificationHistory(filterStr) {
  requirePermission_('notifications.view_history', 'ไม่มีสิทธิ์ดูประวัติการแจ้งเตือน');
  try {
    var filter = typeof filterStr === 'string' ? JSON.parse(filterStr || '{}') : (filterStr || {});
    var limit = Math.max(1, Math.min(300, Number(filter.limit) || 100));
    var statusFilter = String(filter.status || 'all').trim().toUpperCase();
    var cacheKey = 'notificationHistory::' + limit + '::' + (statusFilter || 'ALL');
    var cached = getJsonCache_(cacheKey);
    if (cached && Array.isArray(cached.records)) {
      return { status: 'Success', records: cached.records, cached: true };
    }

    var sheet = ensureNotificationHistorySheet_(getDatabaseSpreadsheet_());
    if (sheet.getLastRow() <= 1) return { status: 'Success', records: [] };

    var values = readSheetRowsFromBottomFiltered_(
      sheet,
      2,
      NOTIFICATION_HISTORY_HEADERS.length,
      limit,
      statusFilter === 'ALL' ? null : function(row) {
        return String(row[4] || '').trim().toUpperCase() === statusFilter;
      },
      200
    );

    var rows = values.map(function(row) {
      return {
        notificationId: String(row[0] || '').trim(),
        flagKey: String(row[1] || '').trim(),
        subject: String(row[2] || '').trim(),
        recipients: String(row[3] || '').trim(),
        status: String(row[4] || '').trim().toUpperCase(),
        bodyPreview: String(row[5] || '').trim(),
        relatedRef: String(row[6] || '').trim(),
        actorUsername: String(row[7] || '').trim(),
        actorName: String(row[8] || '').trim(),
        createdAt: String(row[9] || '').trim(),
        errorMessage: String(row[10] || '').trim()
      };
    });

    putJsonCache_(cacheKey, { records: rows }, 20);
    return { status: 'Success', records: rows, cached: false };
  } catch (e) {
    return { status: 'Error', message: 'โหลดประวัติการแจ้งเตือนไม่สำเร็จ: ' + e.toString(), records: [] };
  }
}

function getExecutiveDashboard(filterStr) {
  requirePermission_('dashboard.executive', 'ไม่มีสิทธิ์ดูแดชบอร์ดผู้บริหาร');
  try {
    var filter = typeof filterStr === 'string' ? JSON.parse(filterStr || '{}') : (filterStr || {});
    var topLimit = Math.max(3, Math.min(20, Number(filter.topLimit) || 8));
    var cacheKey = 'executiveDashboard::top' + topLimit;
    var cached = getJsonCache_(cacheKey);
    if (cached && cached.summary) {
      return {
        status: 'Success',
        summary: cached.summary || null,
        recentMonths: cached.recentMonths || [],
        topOutstanding: cached.topOutstanding || [],
        approvalSummary: cached.approvalSummary || null,
        notificationSummary: cached.notificationSummary || null,
        attachmentSummary: cached.attachmentSummary || null,
        cached: true
      };
    }

    var ss = getDatabaseSpreadsheet_();
    var runtimeSnapshot = getJsonCache_('appRuntimeSnapshot');
    if (!runtimeSnapshot || !Array.isArray(runtimeSnapshot.loans)) {
      runtimeSnapshot = buildAppRuntimeSnapshot_(ss, new Date());
      putJsonCache_('appRuntimeSnapshot', runtimeSnapshot, 120);
    }

    var loans = Array.isArray(runtimeSnapshot.loans) ? runtimeSnapshot.loans.map(function(item) {
      return {
        memberId: String(item.memberId || '').trim(),
        contract: String(item.contract || '').trim(),
        member: String(item.member || '').trim(),
        amount: Number(item.amount) || 0,
        balance: Number(item.balance) || 0,
        status: String(item.status || '').trim(),
        createdAt: normalizeLoanCreatedAt_(item.createdAt)
      };
    }) : [];

    var runtimeDashboardOverview = runtimeSnapshot && runtimeSnapshot.dashboardOverviewStats ? runtimeSnapshot.dashboardOverviewStats : buildDashboardOverviewStatsFromLoans_(loans, 0);
    var activeLoans = loans.filter(function(item) {
      return (Number(item.balance) || 0) > 0 && String(item.status || '').trim() !== 'ปิดบัญชี' && String(item.status || '').indexOf('กลบหนี้') === -1;
    });
    var monthlyCoverCards = Array.isArray(runtimeSnapshot.monthlyDashboardCoverCards) ? runtimeSnapshot.monthlyDashboardCoverCards : [];
    var recentMonths = monthlyCoverCards.slice(Math.max(0, monthlyCoverCards.length - 6)).map(function(card) {
      return {
        month: Number(card.month) || 0,
        yearBe: Number(card.yearBe) || 0,
        monthLabel: String(card.monthLabel || ''),
        totalSent: Number(card.totalSent) || 0,
        principalPaid: Number(card.principalPaid) || 0,
        interestPaid: Number(card.interestPaid) || 0,
        offsetPaid: Number(card.offsetPaid) || 0,
        transactionCount: Number(card.transactionCount) || 0,
        memberCount: Number(card.memberCount) || 0,
        contractCount: Number(card.contractCount) || 0
      };
    });

    var cachedAppData = getCachedAppDataSnapshot_();
    var totalMembers = Number(runtimeDashboardOverview.totalMembers) || 0;
    if (!(totalMembers > 0)) {
      totalMembers = cachedAppData && Array.isArray(cachedAppData.members)
        ? cachedAppData.members.length
        : (function() {
            var membersSheet = ss.getSheetByName('Members');
            return (membersSheet && membersSheet.getLastRow() > 1) ? (membersSheet.getLastRow() - 1) : 0;
          })();
    }

    var approvalSummary = getApprovalQueueSummaryCached_(ss);
    var notificationSummary = getNotificationSummaryCached_(ss);
    var attachmentSummary = getAttachmentSummaryCached_(ss);

    var topOutstanding = activeLoans.sort(function(a, b) {
      return (Number(b.balance) || 0) - (Number(a.balance) || 0);
    }).slice(0, topLimit);

    var summary = {
      totalMembers: totalMembers,
      totalLoans: loans.length,
      activeLoans: Number(runtimeDashboardOverview.activeLoanCount) || activeLoans.length,
      totalLoanAmount: Number(runtimeDashboardOverview.totalLoanAmount) || loans.reduce(function(sum, item) { return sum + (Number(item.amount) || 0); }, 0),
      totalOutstandingBalance: Number(runtimeDashboardOverview.totalOutstandingBalance) || activeLoans.reduce(function(sum, item) { return sum + (Number(item.balance) || 0); }, 0),
      pendingApprovals: approvalSummary.pending,
      sentNotifications: notificationSummary.sent,
      activeAttachments: attachmentSummary.totalActive,
      nplCount: Number(runtimeDashboardOverview.nplCount) || 0,
      nplRatio: String(runtimeDashboardOverview.nplRatio || '0.0')
    };

    var payload = {
      status: 'Success',
      summary: summary,
      recentMonths: recentMonths,
      topOutstanding: topOutstanding,
      approvalSummary: approvalSummary,
      notificationSummary: notificationSummary,
      attachmentSummary: attachmentSummary,
      cached: false
    };
    putJsonCache_(cacheKey, {
      summary: summary,
      recentMonths: recentMonths,
      topOutstanding: topOutstanding,
      approvalSummary: approvalSummary,
      notificationSummary: notificationSummary,
      attachmentSummary: attachmentSummary
    }, 30);
    return payload;
  } catch (e) {
    return {
      status: 'Error',
      message: 'โหลดแดชบอร์ดผู้บริหารไม่สำเร็จ: ' + e.toString(),
      summary: null,
      recentMonths: [],
      topOutstanding: [],
      approvalSummary: null,
      notificationSummary: null,
      attachmentSummary: null
    };
  }
}

// ==========================================
// PHASE 8: AUTO REPORT PDF + DETAILED NOTIFICATIONS + TREND DASHBOARD
// ==========================================
var __phase8_orig_getNotificationSettings_ = getNotificationSettings_;
getNotificationSettings_ = function(ss) {
  var settings = __phase8_orig_getNotificationSettings_(ss) || {};
  var spreadsheet = ss || getDatabaseSpreadsheet_();
  var settingsSheet = spreadsheet.getSheetByName('Settings') || createSheetSafely_(spreadsheet, 'Settings', 3, 2, 'เตรียมชีต Settings');
  var parseBool = function(key, defaultValue) {
    var raw = String(getSettingValue_(settingsSheet, key) || '').trim().toLowerCase();
    if (!raw) return !!defaultValue;
    return raw === 'true' || raw === '1' || raw === 'yes';
  };
  settings.notifyPaymentCreated = parseBool('NotifyPaymentCreated', false);
  settings.notifyPaymentReversed = parseBool('NotifyPaymentReversed', true);
  settings.notifyReportArchived = parseBool('NotifyReportArchived', true);
  settings.notifyAccountingClosed = parseBool('NotifyAccountingClosed', true);
  settings.notifyBackupCompleted = parseBool('NotifyBackupCompleted', true);
  return settings;
};

var __phase8_orig_saveSettings = saveSettings;
saveSettings = function(settingsStr) {
  requireAdminSession_();
  try {
    var settingsData = typeof settingsStr === 'string' ? JSON.parse(settingsStr || '{}') : (settingsStr || {});
    var result = __phase8_orig_saveSettings(JSON.stringify(settingsData));
    if (String(result || '').indexOf('Error:') === 0) return result;

    if (settingsData.notificationSettings) {
      var ss = getDatabaseSpreadsheet_();
      var settingsSheet = ss.getSheetByName('Settings') || createSheetSafely_(ss, 'Settings', 3, 2, 'เตรียมชีต Settings');
      var notificationSettings = settingsData.notificationSettings || {};
      upsertSettingValue_(settingsSheet, 'NotifyPaymentCreated', !!notificationSettings.notifyPaymentCreated);
      upsertSettingValue_(settingsSheet, 'NotifyPaymentReversed', !!notificationSettings.notifyPaymentReversed);
      upsertSettingValue_(settingsSheet, 'NotifyReportArchived', !!notificationSettings.notifyReportArchived);
      upsertSettingValue_(settingsSheet, 'NotifyAccountingClosed', !!notificationSettings.notifyAccountingClosed);
      upsertSettingValue_(settingsSheet, 'NotifyBackupCompleted', !!notificationSettings.notifyBackupCompleted);
      appendAuditLogSafe_(ss, {
        action: 'UPDATE_NOTIFICATION_SETTINGS_PHASE8',
        entityType: 'SETTINGS',
        entityId: 'NotificationSettings',
        after: getNotificationSettings_(ss),
        details: 'อัปเดตการตั้งค่าการแจ้งเตือนเหตุการณ์สำคัญแบบละเอียด'
      });
      clearAppCache_();
    }
    return result;
  } catch (e) {
    return 'Error: ' + e.toString();
  }
};


// ==========================================
// DEBUG FIX: ALIGN SETTINGS PERMISSIONS + SAFE SETTINGS SAVE
// ==========================================
var __debugfix_phase8_orig_saveSettings = saveSettings;
saveSettings = function(settingsStr) {
  try {
    var settingsData = typeof settingsStr === 'string' ? JSON.parse(settingsStr || '{}') : (settingsStr || {});
    var hasOwn = function(obj, key) {
      return obj && Object.prototype.hasOwnProperty.call(obj, key);
    };
    var wantsNotificationSettings = hasOwn(settingsData, 'notificationSettings');
    var wantsInterestRate = hasOwn(settingsData, 'interestRate');
    var wantsReportLayoutSettings = hasOwn(settingsData, 'reportLayoutSettings');

    if (!wantsNotificationSettings && !wantsInterestRate && !wantsReportLayoutSettings) {
      requirePermission_('settings.manage', 'ไม่มีสิทธิ์แก้ไขการตั้งค่าระบบ');
    } else if (wantsInterestRate || wantsReportLayoutSettings) {
      requirePermission_('settings.manage', 'ไม่มีสิทธิ์แก้ไขการตั้งค่าระบบ');
    } else if (wantsNotificationSettings) {
      try {
        requirePermission_('notifications.manage', 'ไม่มีสิทธิ์จัดการการแจ้งเตือนอีเมล');
      } catch (notificationPermissionError) {
        requirePermission_('settings.manage', 'ไม่มีสิทธิ์จัดการการแจ้งเตือนอีเมล');
      }
    }

    var ss = getDatabaseSpreadsheet_();
    var settingsSheet = ss.getSheetByName('Settings') || createSheetSafely_(ss, 'Settings', 3, 2, 'เตรียมชีต Settings');
    if (settingsSheet.getLastRow() === 0) {
      ensureSheetGridSizeSafely_(settingsSheet, 1, 2, 'เตรียมชีต Settings');
      settingsSheet.getRange(1, 1, 1, 2).setValues([["หัวข้อการตั้งค่า", "ค่าการตั้งค่า"]]).setFontWeight("bold");
    }

    if (wantsInterestRate) {
      var normalizedInterestRate = Number(settingsData.interestRate);
      if (!isFinite(normalizedInterestRate) || normalizedInterestRate <= 0) {
        normalizedInterestRate = Number(getSettingValue_(settingsSheet, 'InterestRate')) || 12.0;
      }
      upsertSettingValue_(settingsSheet, 'InterestRate', normalizedInterestRate);
      appendAuditLogSafe_(ss, {
        action: 'UPDATE_INTEREST_RATE',
        entityType: 'SETTINGS',
        entityId: 'InterestRate',
        after: { interestRate: normalizedInterestRate },
        details: 'อัปเดตอัตราดอกเบี้ยระบบ'
      });
    }

    if (wantsReportLayoutSettings) {
      var normalizedReportLayoutSettings = normalizeReportLayoutSettings_(settingsData.reportLayoutSettings);
      upsertSettingValue_(settingsSheet, REPORT_LAYOUT_SETTINGS_KEY_, JSON.stringify(normalizedReportLayoutSettings));
      appendAuditLogSafe_(ss, {
        action: 'UPDATE_REPORT_LAYOUT_SETTINGS',
        entityType: 'SETTINGS',
        entityId: 'ReportLayoutSettings',
        after: normalizedReportLayoutSettings,
        details: 'อัปเดตระยะขอบกระดาษและความกว้างคอลัมน์รายงาน'
      });
    }

    if (wantsNotificationSettings) {
      var notificationSettings = settingsData.notificationSettings || {};
      upsertSettingValue_(settingsSheet, 'NotificationRecipients', String(notificationSettings.recipients || '').trim());
      upsertSettingValue_(settingsSheet, 'NotifyApprovalSubmitted', !!notificationSettings.notifyApprovalSubmitted);
      upsertSettingValue_(settingsSheet, 'NotifyApprovalApproved', !!notificationSettings.notifyApprovalApproved);
      upsertSettingValue_(settingsSheet, 'NotifyApprovalRejected', !!notificationSettings.notifyApprovalRejected);
      upsertSettingValue_(settingsSheet, 'NotifyAttachmentUploaded', !!notificationSettings.notifyAttachmentUploaded);
      upsertSettingValue_(settingsSheet, 'NotifyPaymentCreated', !!notificationSettings.notifyPaymentCreated);
      upsertSettingValue_(settingsSheet, 'NotifyPaymentReversed', !!notificationSettings.notifyPaymentReversed);
      upsertSettingValue_(settingsSheet, 'NotifyReportArchived', !!notificationSettings.notifyReportArchived);
      upsertSettingValue_(settingsSheet, 'NotifyAccountingClosed', !!notificationSettings.notifyAccountingClosed);
      upsertSettingValue_(settingsSheet, 'NotifyBackupCompleted', !!notificationSettings.notifyBackupCompleted);
      appendAuditLogSafe_(ss, {
        action: 'UPDATE_NOTIFICATION_SETTINGS',
        entityType: 'SETTINGS',
        entityId: 'NotificationSettings',
        after: getNotificationSettings_(ss),
        details: 'อัปเดตการตั้งค่าการแจ้งเตือนอีเมล'
      });
    }

    clearAppCache_();
    return 'Success';
  } catch (e) {
    return 'Error: ' + e.toString();
  }
};

function buildAutoGeneratedReportLines_(payload) {
  var lines = [];
  var meta = normalizeReportArchivePayload_(payload || {});
  var snapshot = (payload && payload.snapshot) || {};
  lines.push(meta.title || 'รายงานทางการเงิน');
  lines.push('ประเภท: ' + (meta.reportTypeLabel || meta.reportType || '-'));
  lines.push('ช่วงข้อมูล: ' + (meta.periodLabel || meta.reportDateDisplay || '-'));
  if (meta.memberId) lines.push('รหัสสมาชิก: ' + meta.memberId + (meta.memberName ? ' | ' + meta.memberName : ''));
  lines.push('สร้างเอกสารอัตโนมัติเมื่อ: ' + getThaiDate(new Date()).dateTime);
  lines.push('');

  if (meta.reportType === 'daily') {
    var summary = snapshot.summary || {};
    var details = Array.isArray(snapshot.details) ? snapshot.details : [];
    lines.push('สรุปรายงานส่งบัญชี');
    lines.push('หนี้ยกมา: ' + formatNumberForDisplay_(summary.bfwBalance || 0));
    lines.push('รับชำระเงินต้น: ' + formatNumberForDisplay_(summary.principalPaid || 0));
    lines.push('รับชำระกลบหนี้: ' + formatNumberForDisplay_(summary.offsetPaid || 0));
    lines.push('รับชำระดอกเบี้ย: ' + formatNumberForDisplay_(summary.interestPaid || 0));
    lines.push('รวมรับชำระ: ' + formatNumberForDisplay_(summary.totalPaid || 0));
    lines.push('หนี้ยกไป: ' + formatNumberForDisplay_(summary.cfwBalance || 0));
    lines.push('');
    lines.push('รายละเอียดสูงสุด 120 รายการ');
    for (var i = 0; i < Math.min(details.length, 120); i++) {
      var row = details[i] || {};
      lines.push([
        (i + 1) + '.',
        String(row.memberId || '-'),
        String(row.name || '-'),
        'สัญญา ' + String(row.contract || '-'),
        'ต้น ' + formatNumberForDisplay_(row.displayPrincipalPaid || row.totalPrincipalReduction || row.principalPaid || 0),
        'ดอก ' + formatNumberForDisplay_(row.interestPaid || 0),
        'คงเหลือ ' + formatNumberForDisplay_(row.balance || 0),
        'หมายเหตุ ' + String(row.note || '-')
      ].join(' | '));
    }
    if (details.length > 120) lines.push('... ตัดรายละเอียดที่เหลืออีก ' + (details.length - 120) + ' รายการ');
  } else if (meta.reportType === 'register') {
    var member = snapshot.member || {};
    var loanRecords = Array.isArray(snapshot.loanRecords) ? snapshot.loanRecords : [];
    lines.push('รายงานทะเบียนคุม');
    lines.push('สมาชิก: ' + String(member.id || meta.memberId || '-') + ' | ' + String(member.name || meta.memberName || '-'));
    lines.push('จำนวนสัญญา: ' + loanRecords.length);
    lines.push('');
    for (var lr = 0; lr < loanRecords.length; lr++) {
      var loanRecord = loanRecords[lr] || {};
      var loan = loanRecord.loan || {};
      lines.push('สัญญา ' + String(loan.contract || '-') + ' | วงเงิน ' + formatNumberForDisplay_(loan.amount || 0) + ' | คงเหลือ ' + formatNumberForDisplay_(loan.balance || 0));
      lines.push('ผู้ค้ำ: ' + String(loanRecord.guarantors || '-'));
      var history = Array.isArray(loanRecord.history) ? loanRecord.history : [];
      for (var h = 0; h < Math.min(history.length, 24); h++) {
        var tx = history[h] || {};
        lines.push('  - ' + String(tx.date || '-') + ' | ต้น ' + formatNumberForDisplay_(tx.principal || 0) + ' | ดอก ' + formatNumberForDisplay_(tx.interest || 0) + ' | คงเหลือ ' + formatNumberForDisplay_(tx.balance || 0) + ' | ' + String(tx.note || '-'));
      }
      if (history.length > 24) lines.push('  ... ตัดประวัติที่เหลืออีก ' + (history.length - 24) + ' รายการ');
      lines.push('');
    }
  } else {
    var monthlySummary = snapshot.summary || {};
    var monthlyDetails = Array.isArray(snapshot.details) ? snapshot.details : [];
    lines.push('รายงานหนี้คงค้าง');
    lines.push('ยอดคงเหลือรวม: ' + formatNumberForDisplay_(monthlySummary.totalBalance || 0));
    lines.push('จำนวนสัญญารวม: ' + formatNumberForDisplay_(monthlySummary.totalLoans || 0));
    lines.push('จำนวนสัญญาค้างดอก: ' + formatNumberForDisplay_(monthlySummary.overdueLoans || 0));
    lines.push('');
    for (var md = 0; md < Math.min(monthlyDetails.length, 150); md++) {
      var item = monthlyDetails[md] || {};
      lines.push([
        (md + 1) + '.',
        String(item.memberId || '-'),
        String(item.name || '-'),
        'สัญญา ' + String(item.contract || '-'),
        'คงเหลือ ' + formatNumberForDisplay_(item.balance || 0),
        'ค้างดอก ' + formatNumberForDisplay_(item.missedMonths || 0) + ' งวด',
        'สถานะ ' + String(item.status || '-')
      ].join(' | '));
    }
    if (monthlyDetails.length > 150) lines.push('... ตัดรายละเอียดที่เหลืออีก ' + (monthlyDetails.length - 150) + ' รายการ');
  }

  return lines;
}

// phase cleanup: removed earlier duplicate PDF generator declarations.
// The canonical implementations of createAutoGeneratedReportPdfBase64_() and
// generateAndStoreReportPdfAuto() remain later in this file to avoid override drift.

function buildFinancialNotificationLines_(title, pairs) {
  var lines = [String(title || 'แจ้งเตือนระบบการเงิน')];
  for (var i = 0; i < pairs.length; i++) {
    var item = pairs[i];
    if (!item || item[1] === undefined || item[1] === null || item[1] === '') continue;
    lines.push(String(item[0] || '-') + ': ' + String(item[1] || '-'));
  }
  return lines;
}

var __phase8_orig_savePayment = savePayment;
savePayment = function(paymentDataStr) {
  var result = __phase8_orig_savePayment(paymentDataStr);
  try {
    if (result && result.status === 'Success' && !result.duplicated) {
      var tx = result.transaction || {};
      sendNotificationEmailIfEnabled_(getDatabaseSpreadsheet_(), 'notifyPaymentCreated', 'แจ้งเตือน: มีการบันทึกรับชำระใหม่', buildFinancialNotificationLines_('ระบบบันทึกรับชำระใหม่', [
        ['รหัสรายการ', result.transactionId || ''],
        ['สัญญา', tx.contract || ''],
        ['สมาชิก', tx.memberId ? tx.memberId + ' | ' + String(tx.memberName || '') : (tx.memberName || '')],
        ['เงินต้น', formatNumberForDisplay_(tx.principalPaid || 0)],
        ['ดอกเบี้ย', formatNumberForDisplay_(tx.interestPaid || 0)],
        ['คงเหลือใหม่', formatNumberForDisplay_(tx.newBalance || 0)],
        ['เวลา', result.timestamp || '']
      ]), { relatedRef: result.transactionId || '' });
    }
  } catch (notifyError) {
    console.error('phase8 savePayment notify failed', notifyError);
  }
  return result;
};

var __phase8_orig_cancelPayment = cancelPayment;
cancelPayment = function(transactionId, reason) {
  var result = __phase8_orig_cancelPayment(transactionId, reason);
  try {
    if (result && result.status === 'Success') {
      sendNotificationEmailIfEnabled_(getDatabaseSpreadsheet_(), 'notifyPaymentReversed', 'แจ้งเตือน: มีการกลับรายการรับชำระ', buildFinancialNotificationLines_('ระบบกลับรายการรับชำระ', [
        ['รายการอ้างอิง', transactionId || ''],
        ['เหตุผล', reason || ''],
        ['ผลลัพธ์', result.message || 'สำเร็จ']
      ]), { relatedRef: transactionId || '' });
    }
  } catch (notifyError) {
    console.error('phase8 cancelPayment notify failed', notifyError);
  }
  return result;
};

var __phase8_orig_deleteManagedPaymentRecord = deleteManagedPaymentRecord;
deleteManagedPaymentRecord = function(recordId, reason) {
  var result = __phase8_orig_deleteManagedPaymentRecord(recordId, reason);
  try {
    if (result && result.status === 'Success') {
      sendNotificationEmailIfEnabled_(getDatabaseSpreadsheet_(), 'notifyPaymentReversed', 'แจ้งเตือน: มีการกลับรายการรับชำระย้อนหลัง', buildFinancialNotificationLines_('ระบบกลับรายการรับชำระย้อนหลัง', [
        ['รายการอ้างอิง', recordId || ''],
        ['เหตุผล', reason || ''],
        ['ผลลัพธ์', result.message || 'สำเร็จ']
      ]), { relatedRef: recordId || '' });
    }
  } catch (notifyError) {
    console.error('phase8 deleteManagedPaymentRecord notify failed', notifyError);
  }
  return result;
};

var __phase8_orig_saveReportPdfArchive = saveReportPdfArchive;
saveReportPdfArchive = function(payloadStr) {
  var result = __phase8_orig_saveReportPdfArchive(payloadStr);
  try {
    if (result && result.status === 'Success') {
      sendNotificationEmailIfEnabled_(getDatabaseSpreadsheet_(), 'notifyReportArchived', 'แจ้งเตือน: มีการบันทึกรายงาน PDF', buildFinancialNotificationLines_('ระบบบันทึกรายงาน PDF เข้าคลัง', [
        ['เลขเอกสาร', result.documentNo || ''],
        ['ประเภทรายงาน', result.reportTypeLabel || result.reportType || ''],
        ['ชื่อไฟล์', result.fileName || ''],
        ['โฟลเดอร์', result.folderPath || ''],
        ['เวลา', result.createdAt || '']
      ]), { relatedRef: result.documentNo || result.archiveKey || '' });
    }
  } catch (notifyError) {
    console.error('phase8 saveReportPdfArchive notify failed', notifyError);
  }
  return result;
};

var __phase8_orig_backupTransactionsToMonthlySheets = backupTransactionsToMonthlySheets;
backupTransactionsToMonthlySheets = function() {
  var result = __phase8_orig_backupTransactionsToMonthlySheets();
  try {
    if (result && result.status === 'Success') {
      sendNotificationEmailIfEnabled_(getDatabaseSpreadsheet_(), 'notifyBackupCompleted', 'แจ้งเตือน: Backup ธุรกรรมสำเร็จ', buildFinancialNotificationLines_('ระบบ Backup ธุรกรรมสำเร็จ', [
        ['จำนวนรายการที่ย้าย', formatNumberForDisplay_(result.archivedTransactions || 0)],
        ['รายการซ้ำที่ข้าม', formatNumberForDisplay_(result.alreadyArchivedTransactions || 0)],
        ['ชีตปลายทาง', result.archiveSheetName || ''],
        ['เวลา', getThaiDate(new Date()).dateTime]
      ]), { relatedRef: result.archiveSheetName || '' });
    }
  } catch (notifyError) {
    console.error('phase8 backup notify failed', notifyError);
  }
  return result;
};

var __phase8_orig_closeAccountingPeriod = closeAccountingPeriod;
closeAccountingPeriod = function() {
  var result = __phase8_orig_closeAccountingPeriod();
  try {
    if (result && result.status === 'Success') {
      sendNotificationEmailIfEnabled_(getDatabaseSpreadsheet_(), 'notifyAccountingClosed', 'แจ้งเตือน: ปิดยอดบัญชีสำเร็จ', buildFinancialNotificationLines_('ระบบปิดยอดบัญชีสำเร็จ', [
        ['จำนวนสัญญาที่ตรวจ', formatNumberForDisplay_(result.processedLoans || 0)],
        ['จำนวนสัญญาที่เปลี่ยน', formatNumberForDisplay_(result.changedLoans || 0)],
        ['จำนวนสัญญาค้างดอก', formatNumberForDisplay_(result.totalArrearsLoans || 0)],
        ['รอบบัญชี', result.closedMonthKey || ''],
        ['เวลา', getThaiDate(new Date()).dateTime]
      ]), { relatedRef: result.closedMonthKey || '' });
    }
  } catch (notifyError) {
    console.error('phase8 close accounting notify failed', notifyError);
  }
  return result;
};


// ==========================================
// PHASE 6 DEBUG: REPORT MENU / OVERDUE REPORT / AUTO PDF SNAPSHOT
// ==========================================
function getMonthlyOutstandingMemberSortValue_(memberId) {
  var textValue = String(memberId || '').trim();
  var numericMatch = textValue.match(/\d+/);
  if (numericMatch) {
    return { kind: 'number', value: Number(numericMatch[0]) || 0 };
  }
  return { kind: 'text', value: textValue.toLowerCase() };
}

function getHistoricalBalanceAtReportDate_(loanRow, futurePrincipalPaidAfterDate, latestBalanceOnOrBeforeDate) {
  var latestBalance = Number(latestBalanceOnOrBeforeDate);
  if (isFinite(latestBalance)) {
    return Math.max(0, latestBalance);
  }

  var currentBalance = Number((loanRow && loanRow[5]) || 0);
  var reconstructedBalance = currentBalance + (Number(futurePrincipalPaidAfterDate) || 0);
  return Math.max(0, reconstructedBalance);
}

function compareMonthlyOutstandingRows_(a, b) {
  var missedA = Number((a && a.missedMonths) || 0);
  var missedB = Number((b && b.missedMonths) || 0);
  if (missedA !== missedB) return missedB - missedA;

  var sortA = getMonthlyOutstandingMemberSortValue_(a && a.memberId);
  var sortB = getMonthlyOutstandingMemberSortValue_(b && b.memberId);
  if (sortA.kind !== sortB.kind) return sortA.kind === 'number' ? -1 : 1;
  if (sortA.value < sortB.value) return -1;
  if (sortA.value > sortB.value) return 1;

  var balanceA = Number((a && a.balance) || 0);
  var balanceB = Number((b && b.balance) || 0);
  if (balanceA !== balanceB) return balanceB - balanceA;

  var contractA = String((a && a.contract) || '').trim();
  var contractB = String((b && b.contract) || '').trim();
  return contractA.localeCompare(contractB);
}

function getOverdueInterestReport(reportDateInput) {
  requirePermission_('reports.view', 'ไม่มีสิทธิ์ดูรายงาน');
  try {
    var reportDateParts = parseReportDateInput_(reportDateInput);
    if (!reportDateParts) {
      return { status: 'Error', message: 'รูปแบบงวดรายงานหนี้คงค้างไม่ถูกต้อง' };
    }

    var reportDateNumber = getThaiDateNumberFromParts_(reportDateParts);
    var cacheKey = 'overdueInterestReport::' + String(reportDateParts.dateOnly || '');
    var cached = getJsonCache_(cacheKey);
    if (cached && cached.summary && Array.isArray(cached.details)) {
      return {
        status: 'Success',
        reportDate: cached.reportDate || reportDateParts.dateOnly,
        reportDateDisplay: cached.reportDateDisplay || reportDateParts.dateOnly,
        reportDateNumber: reportDateNumber,
        periodLabel: cached.periodLabel || (getThaiMonthLabelByNumber_(reportDateParts.month) + ' ' + reportDateParts.yearBe),
        asOfLabel: cached.asOfLabel || ('ข้อมูล ณ วันที่ ' + reportDateParts.dateOnly),
        yearBe: reportDateParts.yearBe,
        month: reportDateParts.month,
        summary: cached.summary,
        details: cached.details,
        cached: true
      };
    }

    var ss = ensureRuntimeDatabase_();
    var settingsSheet = ss.getSheetByName('Settings');
    var lastClosedAccountingMonthKey = settingsSheet ? String(getSettingValue_(settingsSheet, 'LastClosedAccountingMonth') || '').trim() : '';
    var includeSelectedMonthInstallment = shouldCountSelectedMonthAsDueForOverdueReport_(reportDateParts.yearBe, reportDateParts.month, lastClosedAccountingMonthKey);
    var loanSheet = getSheetOrThrow(ss, 'Loans');
    var loanRows = loanSheet.getLastRow() > 1
      ? loanSheet.getRange(2, 1, loanSheet.getLastRow() - 1, 12).getValues()
      : [];

    var indexedRecords = getActiveIndexedTransactionRecordsCached_(ss, 0);
    var factsByContract = {};
    var seenKeys = {};

    for (var r = 0; r < indexedRecords.length; r++) {
      var record = indexedRecords[r];
      var contractKey = String(record.contract || '').trim();
      if (!contractKey) continue;

      var dedupeKey = String(record.id || '').trim() || [
        record.timestamp,
        record.contract,
        record.memberId,
        record.principalPaid,
        record.interestPaid,
        record.newBalance,
        record.note,
        record.sourceSheet,
        String(record.rowIndex || 0)
      ].join('|');
      if (seenKeys[dedupeKey]) continue;
      seenKeys[dedupeKey] = true;

      if (!factsByContract[contractKey]) {
        factsByContract[contractKey] = {
          futurePrincipalPaid: 0,
          paidInstallmentsYtd: 0,
          latestRecordOnOrBeforeDate: null,
          lastBalanceOnOrBeforeDate: null
        };
      }

      var facts = factsByContract[contractKey];
      if (Number(record.dateNumber) > reportDateNumber) {
        facts.futurePrincipalPaid += Number(record.principalPaid) || 0;
      } else if (!facts.latestRecordOnOrBeforeDate || compareTransactionRecordOrderAsc_(facts.latestRecordOnOrBeforeDate, record) <= 0) {
        facts.latestRecordOnOrBeforeDate = record;
        facts.lastBalanceOnOrBeforeDate = Number(record.newBalance) || 0;
      }
      if (Number(record.yearBe) === Number(reportDateParts.yearBe) && Number(record.dateNumber) <= reportDateNumber) {
        var installmentCount = Math.max(0, Number(record.interestMonthsPaid) || 0);
        if (!installmentCount && (Number(record.interestPaid) || 0) > 0) installmentCount = 1;
        facts.paidInstallmentsYtd += installmentCount;
      }
    }

    var summary = { totalBalance: 0, totalLoans: 0, overdueLoans: 0 };
    var details = [];

    for (var i = 0; i < loanRows.length; i++) {
      var row = loanRows[i];
      var contractNo = String(row[1] || '').trim();
      if (!contractNo) continue;

      var createdAtNumber = getLoanCreatedAtNumberForReport_(row);
      if (!createdAtNumber || createdAtNumber > reportDateNumber) continue;

      var factsForLoan = factsByContract[contractNo] || { futurePrincipalPaid: 0, paidInstallmentsYtd: 0, latestRecordOnOrBeforeDate: null, lastBalanceOnOrBeforeDate: null };
      var balanceAtReportDate = getHistoricalBalanceAtReportDate_(row, factsForLoan.futurePrincipalPaid, factsForLoan.lastBalanceOnOrBeforeDate);
      if (balanceAtReportDate <= 0) continue;

      var createdParts = parseThaiDateParts_(normalizeLoanCreatedAt_(row[9])) || parseThaiDateParts_(row[9]) || reportDateParts;
      var createdYearBe = Number(createdParts && createdParts.yearBe) || Number(reportDateParts.yearBe);
      var createdMonth = Math.min(12, Math.max(1, Number(createdParts && createdParts.month) || 1));
      var dueMonthsBeforeSelected = 0;
      if (createdYearBe < Number(reportDateParts.yearBe)) {
        dueMonthsBeforeSelected = Math.max(0, (Number(reportDateParts.month) || 0) - 1);
      } else if (createdYearBe === Number(reportDateParts.yearBe)) {
        dueMonthsBeforeSelected = Math.max(0, (Number(reportDateParts.month) || 0) - createdMonth);
      } else {
        continue;
      }

      var dueMonthsYtd = dueMonthsBeforeSelected + (includeSelectedMonthInstallment ? 1 : 0);
      var paidInstallmentsYtd = Math.max(0, Number(factsForLoan.paidInstallmentsYtd) || 0);
      var missedMonths = Math.max(0, dueMonthsYtd - paidInstallmentsYtd);
      var isOverdue = missedMonths >= 1;

      summary.totalBalance += balanceAtReportDate;
      summary.totalLoans += 1;
      if (isOverdue) summary.overdueLoans += 1;

      details.push({
        no: 0,
        memberId: String(row[0] || '').trim() || '-',
        name: String(row[2] || '').trim() || '-',
        nameWithMissed: String(row[2] || '').trim() + (isOverdue ? (' (' + missedMonths + ' งวด)') : ''),
        contract: contractNo,
        amount: Number(row[3]) || 0,
        balance: balanceAtReportDate,
        missedMonths: missedMonths,
        baseMissedMonths: Math.max(0, dueMonthsBeforeSelected - paidInstallmentsYtd),
        currentMonthRequiredInstallment: includeSelectedMonthInstallment ? 1 : 0,
        status: deriveLoanStatusFromState_(balanceAtReportDate, missedMonths, String(row[6] || '')),
        isOverdue: isOverdue
      });
    }

    details.sort(compareMonthlyOutstandingRows_);
    for (var d = 0; d < details.length; d++) {
      details[d].no = d + 1;
    }

    var payload = {
      status: 'Success',
      reportDate: reportDateParts.dateOnly,
      reportDateDisplay: reportDateParts.dateOnly,
      reportDateNumber: reportDateNumber,
      periodLabel: getThaiMonthLabelByNumber_(reportDateParts.month) + ' ' + reportDateParts.yearBe,
      asOfLabel: 'ข้อมูล ณ วันที่ ' + reportDateParts.dateOnly,
      yearBe: reportDateParts.yearBe,
      month: reportDateParts.month,
      summary: summary,
      details: details,
      cached: false
    };
    putJsonCache_(cacheKey, {
      reportDate: payload.reportDate,
      reportDateDisplay: payload.reportDateDisplay,
      periodLabel: payload.periodLabel,
      asOfLabel: payload.asOfLabel,
      summary: summary,
      details: details
    }, 120);
    return payload;
  } catch (e) {
    console.error('getOverdueInterestReport failed', e);
    return { status: 'Error', message: 'โหลดรายงานหนี้คงค้างไม่สำเร็จ: ' + e.toString() };
  }
}

function buildRegisterReportSnapshotForPayload_(payload) {
  var ss = ensureRuntimeDatabase_();
  var normalizedMemberId = normalizeTextForMatch_(payload && payload.memberId);
  if (!normalizedMemberId) return {};

  var reportDateParts = parseReportDateInput_(payload && payload.reportDate) || parseThaiDateParts_(new Date());
  var reportDateNumber = getThaiDateNumberFromParts_(reportDateParts);
  var txResult = getRegisterReportTransactions(String(payload.memberId || ''), payload && payload.reportDate);
  var allTransactions = txResult && txResult.status === 'Success' && Array.isArray(txResult.transactions)
    ? txResult.transactions
    : [];

  var transactionsByContract = {};
  for (var i = 0; i < allTransactions.length; i++) {
    var tx = allTransactions[i] || {};
    var contractNo = String(tx.contract || '').trim();
    if (!contractNo) continue;
    if (!transactionsByContract[contractNo]) transactionsByContract[contractNo] = [];
    transactionsByContract[contractNo].push(tx);
  }
  var txContracts = Object.keys(transactionsByContract);
  for (var c = 0; c < txContracts.length; c++) {
    transactionsByContract[txContracts[c]].sort(compareTransactionRecordOrderAsc_);
  }

  var memberInfo = { id: String(payload.memberId || '').trim(), name: String(payload.memberName || '').trim() };
  var memberSheet = ss.getSheetByName('Members');
  if (memberSheet && memberSheet.getLastRow() > 1) {
    var memberRows = memberSheet.getRange(2, 1, memberSheet.getLastRow() - 1, 3).getValues();
    for (var m = 0; m < memberRows.length; m++) {
      if (normalizeTextForMatch_(memberRows[m][0]) === normalizedMemberId) {
        memberInfo = { id: String(memberRows[m][0] || '').trim(), name: String(memberRows[m][1] || '').trim() };
        break;
      }
    }
  }

  var loanSheet = ss.getSheetByName('Loans');
  var loanRows = loanSheet && loanSheet.getLastRow() > 1
    ? loanSheet.getRange(2, 1, loanSheet.getLastRow() - 1, 12).getValues()
    : [];
  var loanRecords = [];

  for (var l = 0; l < loanRows.length; l++) {
    var row = loanRows[l];
    if (normalizeTextForMatch_(row[0]) !== normalizedMemberId) continue;
    var createdAtNumber = getLoanCreatedAtNumberForReport_(row);
    if (createdAtNumber && createdAtNumber > reportDateNumber) continue;

    var contract = String(row[1] || '').trim();
    var indexedContractTransactions = getIndexedContractTransactionRecords_(ss, contract);
    if (!indexedContractTransactions.length) {
      indexedContractTransactions = transactionsByContract[contract] || [];
    }
    indexedContractTransactions.sort(compareTransactionRecordOrderAsc_);

    var futurePrincipalPaid = 0;
    var history = [];
    var seenHistoryKeys = {};
    for (var t = 0; t < indexedContractTransactions.length; t++) {
      var txItem = indexedContractTransactions[t];
      var txDateNumber = Number(txItem.dateNumber) || 0;
      var dedupeHistoryKey = String(txItem.id || '').trim() || [
        txItem.timestamp,
        txItem.contract,
        txItem.memberId,
        txItem.principalPaid,
        txItem.interestPaid,
        txItem.newBalance,
        txItem.note,
        txItem.sourceSheet,
        String(txItem.rowIndex || 0)
      ].join('|');
      if (seenHistoryKeys[dedupeHistoryKey]) continue;
      seenHistoryKeys[dedupeHistoryKey] = true;

      if (txDateNumber > reportDateNumber) {
        futurePrincipalPaid += Number(txItem.principalPaid) || 0;
        continue;
      }
      history.push({
        date: String(txItem.dateOnly || txItem.timestamp || '').trim(),
        principal: Number(txItem.principalPaid) || 0,
        interest: Number(txItem.interestPaid) || 0,
        balance: Number(txItem.newBalance) || 0,
        note: String(txItem.note || '').trim() || '-'
      });
    }

    var guarantorParts = [];
    if (row[10]) guarantorParts.push(String(row[10] || '').trim());
    if (row[11]) guarantorParts.push(String(row[11] || '').trim());

    loanRecords.push({
      loan: {
        contract: contract,
        amount: Number(row[3]) || 0,
        balance: history.length > 0
          ? Math.max(0, Number(history[history.length - 1].balance) || 0)
          : getHistoricalBalanceAtReportDate_(row, futurePrincipalPaid, null)
      },
      guarantors: guarantorParts.join(', ') || '(ไม่มีข้อมูล)',
      history: history
    });
  }

  loanRecords.sort(function(a, b) {
    return String((a.loan && a.loan.contract) || '').localeCompare(String((b.loan && b.loan.contract) || ''));
  });

  return {
    member: memberInfo,
    loanRecords: loanRecords
  };
}

function buildReportSnapshotForArchivePayload_(payload) {
  var meta = normalizeReportArchivePayload_(payload || {});
  if (meta.reportType === 'daily') {
    var dailyResult = getDailyReport((payload && payload.reportDate) || meta.reportDateDisplay || '');
    return dailyResult && dailyResult.status === 'Success' ? dailyResult : {};
  }
  if (meta.reportType === 'register') {
    return buildRegisterReportSnapshotForPayload_(payload || {});
  }
  var overdueResult = getOverdueInterestReport((payload && payload.reportDate) || meta.reportDateDisplay || '');
  return overdueResult && overdueResult.status === 'Success' ? overdueResult : {};
}

function generateAndStoreReportPdfAuto(payloadStr) {
  requirePermission_('reports.archive', 'ไม่มีสิทธิ์บันทึกรายงาน PDF');
  try {
    var payload = typeof payloadStr === 'string' ? JSON.parse(payloadStr || '{}') : (payloadStr || {});
    payload.snapshot = buildReportSnapshotForArchivePayload_(payload);
    var incomingPdfBase64 = String(payload && payload.pdfBase64 || '').trim();
    if (incomingPdfBase64) {
      payload.pdfBase64 = incomingPdfBase64;
      payload.mimeType = String(payload && payload.mimeType || 'application/pdf');
    } else {
      payload.pdfBase64 = createAutoGeneratedReportPdfBase64_(payload);
      payload.mimeType = 'application/pdf';
    }
    var result = saveReportPdfArchive(JSON.stringify(payload));
    if (result && result.status === 'Success') {
      result.autoGenerated = true;
      result.message = incomingPdfBase64
        ? 'สร้างและบันทึกรายงาน PDF อัตโนมัติจากโครงหน้ารายงานล่าสุดเรียบร้อยแล้ว'
        : 'สร้างและบันทึกรายงาน PDF อัตโนมัติเรียบร้อยแล้ว';
    }
    return result;
  } catch (e) {
    console.error('generateAndStoreReportPdfAuto phase6 failed', e);
    return { status: 'Error', message: 'สร้างรายงาน PDF อัตโนมัติไม่สำเร็จ: ' + e.toString() };
  }
}



function formatPdfMoneyValue_(value) {
  var num = Number(value) || 0;
  return num ? formatNumberForDisplay_(num) : '';
}

function getPdfMemberSortValue_(memberId) {
  var textValue = String(memberId || '').trim();
  var numericMatch = textValue.match(/\d+/);
  if (numericMatch) return { kind: 'number', value: Number(numericMatch[0]) };
  return { kind: 'text', value: textValue.toLowerCase() };
}

function compareDailyPdfRows_(a, b) {
  var sortA = getPdfMemberSortValue_(a && a.memberId);
  var sortB = getPdfMemberSortValue_(b && b.memberId);
  if (sortA.kind !== sortB.kind) return sortA.kind === 'number' ? -1 : 1;
  if (sortA.value < sortB.value) return -1;
  if (sortA.value > sortB.value) return 1;
  var balanceA = Number((a && a.balance) || 0);
  var balanceB = Number((b && b.balance) || 0);
  if (balanceA !== balanceB) return balanceB - balanceA;
  return String((a && a.contract) || '').localeCompare(String((b && b.contract) || ''));
}

function trySetReportDocMargins_(body) {
  if (!body) return;
  try { if (typeof body.setMarginTop === 'function') body.setMarginTop(12); } catch (e) {}
  try { if (typeof body.setMarginBottom === 'function') body.setMarginBottom(12); } catch (e) {}
  try { if (typeof body.setMarginLeft === 'function') body.setMarginLeft(18); } catch (e) {}
  try { if (typeof body.setMarginRight === 'function') body.setMarginRight(18); } catch (e) {}
}

function styleReportParagraph_(paragraph, options) {
  if (!paragraph) return;
  var opts = options || {};
  try { paragraph.setBold(!!opts.bold); } catch (e) {}
  try { paragraph.setFontSize(opts.fontSize || 9); } catch (e) {}
  try { paragraph.setSpacingBefore(opts.spacingBefore != null ? opts.spacingBefore : 0); } catch (e) {}
  try { paragraph.setSpacingAfter(opts.spacingAfter != null ? opts.spacingAfter : 0); } catch (e) {}
  try { paragraph.setLineSpacing(opts.lineSpacing || 1); } catch (e) {}
  try { if (opts.alignment) paragraph.setAlignment(opts.alignment); } catch (e) {}
}

function setReportCellText_(cell, text, options) {
  if (!cell) return;
  cell.clear();
  var paragraph = cell.appendParagraph(String(text == null ? '' : text));
  var opts = options || {};
  styleReportParagraph_(paragraph, {
    bold: !!opts.bold,
    fontSize: opts.fontSize || 8,
    spacingBefore: 0,
    spacingAfter: 0,
    lineSpacing: 1,
    alignment: opts.alignment || DocumentApp.HorizontalAlignment.LEFT
  });
  try { cell.setBackgroundColor(opts.backgroundColor || null); } catch (e) {}
}

function appendReportSummaryPairs_(body, pairs) {
  for (var i = 0; i < pairs.length; i++) {
    var item = pairs[i] || [];
    var p = body.appendParagraph(String(item[0] || '') + ' : ' + String(item[1] || ''));
    styleReportParagraph_(p, { fontSize: 10, spacingBefore: 0, spacingAfter: 0 });
  }
}

function appendReportCoverPage_(body, title, subtitle, summaryPairs) {
  var titleParagraph = body.appendParagraph(String(title || 'รายงาน'));
  styleReportParagraph_(titleParagraph, { bold: true, fontSize: 16, alignment: DocumentApp.HorizontalAlignment.CENTER, spacingAfter: 4 });
  if (subtitle) {
    var subtitleParagraph = body.appendParagraph(String(subtitle || ''));
    styleReportParagraph_(subtitleParagraph, { fontSize: 11, alignment: DocumentApp.HorizontalAlignment.CENTER, spacingAfter: 8 });
  }
  appendReportSummaryPairs_(body, summaryPairs || []);
}

function appendReportTablePage_(body, headerLines, headers, rows, footerRow, alignmentList) {
  for (var h = 0; h < headerLines.length; h++) {
    var line = headerLines[h] || {};
    var p = body.appendParagraph(String(line.text || ''));
    styleReportParagraph_(p, {
      bold: !!line.bold,
      fontSize: line.fontSize || 9,
      alignment: line.alignment || DocumentApp.HorizontalAlignment.LEFT,
      spacingBefore: line.spacingBefore != null ? line.spacingBefore : 0,
      spacingAfter: line.spacingAfter != null ? line.spacingAfter : 0
    });
  }
  var tableRows = [headers].concat(rows || []);
  if (footerRow) tableRows.push(footerRow);
  var table = body.appendTable(tableRows);
  try { table.setBorderWidth(1); } catch (e) {}
  for (var r = 0; r < table.getNumRows(); r++) {
    var row = table.getRow(r);
    var isHeader = r === 0;
    var isFooter = !!footerRow && r === table.getNumRows() - 1;
    for (var c = 0; c < row.getNumCells(); c++) {
      var cell = row.getCell(c);
      var align = alignmentList[c] || DocumentApp.HorizontalAlignment.LEFT;
      var value = (tableRows[r] && tableRows[r][c] !== undefined && tableRows[r][c] !== null) ? tableRows[r][c] : '';
      setReportCellText_(cell, value, {
        bold: isHeader || isFooter,
        fontSize: isHeader ? 8 : 8,
        alignment: align,
        backgroundColor: isHeader ? '#f1f5f9' : isFooter ? '#f8fafc' : null
      });
    }
  }
}

function buildPagedRowsForPdf_(sourceRows, rowsPerPage, blankFactory) {
  var pages = [];
  var safeRows = Array.isArray(sourceRows) ? sourceRows.slice() : [];
  if (!safeRows.length) safeRows = [blankFactory()];
  for (var start = 0; start < safeRows.length; start += rowsPerPage) {
    var rows = safeRows.slice(start, start + rowsPerPage);
    while (rows.length < rowsPerPage) rows.push(blankFactory());
    pages.push(rows);
  }
  return pages;
}

function createAutoGeneratedReportPdfBase64_(payload) {
  var meta = normalizeReportArchivePayload_(payload || {});
  var snapshot = (payload && payload.snapshot) || buildReportSnapshotForArchivePayload_(payload || {}) || {};
  var doc = DocumentApp.create('TMP ' + String(meta.title || 'Report'));
  try {
    var body = doc.getBody();
    body.clear();
    trySetReportDocMargins_(body);
    var rowsPerPage = 34;

    if (meta.reportType === 'daily') {
      var dailySummary = (snapshot && snapshot.summary) || {};
      var dailyDetails = Array.isArray(snapshot && snapshot.details) ? snapshot.details.slice() : [];
      dailyDetails.sort(compareDailyPdfRows_);
      for (var di = 0; di < dailyDetails.length; di++) dailyDetails[di].no = di + 1;
      appendReportCoverPage_(body, 'รายงานส่งบัญชี กลุ่มออมทรัพย์เพื่อการผลิตบ้านพิตำ', 'ประจำวันที่ ' + String(meta.reportDateDisplay || ''), [
        ['ยอดยกมา', formatPdfMoneyValue_(dailySummary.bfwBalance)],
        ['ชำระเงินต้น', formatPdfMoneyValue_(dailySummary.principalPaid)],
        ['กลบหนี้', formatPdfMoneyValue_(dailySummary.offsetPaid)],
        ['ชำระดอกเบี้ย', formatPdfMoneyValue_(dailySummary.interestPaid)],
        ['ยอดรวมส่งบัญชี', formatPdfMoneyValue_(dailySummary.totalPaid)],
        ['หนี้ยกไป', formatPdfMoneyValue_(dailySummary.cfwBalance)]
      ]);
      if (Array.isArray(snapshot && snapshot.mixedCaseNotes) && snapshot.mixedCaseNotes.length) {
        body.appendParagraph('หมายเหตุ').setBold(true);
        for (var mn = 0; mn < snapshot.mixedCaseNotes.length; mn++) {
          var noteItem = snapshot.mixedCaseNotes[mn] || {};
          var np = body.appendParagraph(String(noteItem.memberId || '-') + ' - ' + String(noteItem.name || '-') + ' | สัญญา ' + String(noteItem.contract || '-') + ' | ' + String(noteItem.detail || '-'));
          styleReportParagraph_(np, { fontSize: 8, spacingBefore: 0, spacingAfter: 0 });
        }
      }
      body.appendPageBreak();
      var dailyPages = buildPagedRowsForPdf_(dailyDetails, rowsPerPage, function() {
        return { no: '', memberId: '', name: '', broughtForward: 0, principalPaid: 0, offsetPaid: 0, interestPaid: 0, balance: 0, note: '', isBlank: true };
      });
      for (var dp = 0; dp < dailyPages.length; dp++) {
        var pageRows = dailyPages[dp];
        var totals = { bfw: 0, principal: 0, interest: 0, balance: 0 };
        var tableRows = [];
        for (var dr = 0; dr < pageRows.length; dr++) {
          var row = pageRows[dr] || {};
          if (!row.isBlank && row.no !== '') {
            totals.bfw += Number(row.broughtForward) || 0;
            totals.principal += Number((row.totalPrincipalReduction != null ? row.totalPrincipalReduction : ((Number(row.principalPaid) || 0) + (Number(row.offsetPaid) || 0)))) || 0;
            totals.interest += Number(row.interestPaid) || 0;
            totals.balance += Number(row.balance) || 0;
          }
          tableRows.push([
            row.no || '',
            row.memberId || '',
            row.name || '',
            row.isBlank ? '' : formatPdfMoneyValue_(row.broughtForward),
            row.isBlank ? '' : formatPdfMoneyValue_((row.totalPrincipalReduction != null ? row.totalPrincipalReduction : ((Number(row.principalPaid) || 0) + (Number(row.offsetPaid) || 0)))),
            row.isBlank ? '' : formatPdfMoneyValue_(row.interestPaid),
            row.isBlank ? '' : formatPdfMoneyValue_(row.balance),
            row.note || ''
          ]);
        }
        appendReportTablePage_(body, [
          { text: 'รายละเอียดรายงานส่งบัญชี', bold: true, fontSize: 11 },
          { text: 'ประจำวันที่ ' + String(meta.reportDateDisplay || ''), fontSize: 9 },
          { text: 'หน้า ' + (dp + 1) + ' / ' + dailyPages.length + ' | เรียงตามเลขสมาชิกจากน้อยไปมาก', fontSize: 8, alignment: DocumentApp.HorizontalAlignment.RIGHT }
        ], ['ลำดับ', 'รหัสสมาชิก', 'ชื่อ-สกุล', 'หนี้ยกมา', 'ชำระต้น', 'ดอกเบี้ย', 'คงเหลือ', 'หมายเหตุ'], tableRows, ['รวมหน้า', '', '', formatPdfMoneyValue_(totals.bfw), formatPdfMoneyValue_(totals.principal), formatPdfMoneyValue_(totals.interest), formatPdfMoneyValue_(totals.balance), ''], [DocumentApp.HorizontalAlignment.CENTER, DocumentApp.HorizontalAlignment.CENTER, DocumentApp.HorizontalAlignment.LEFT, DocumentApp.HorizontalAlignment.RIGHT, DocumentApp.HorizontalAlignment.RIGHT, DocumentApp.HorizontalAlignment.RIGHT, DocumentApp.HorizontalAlignment.RIGHT, DocumentApp.HorizontalAlignment.CENTER]);
        if (dp < dailyPages.length - 1) body.appendPageBreak();
      }
    } else if (meta.reportType === 'register') {
      var registerSnapshot = snapshot || {};
      var member = registerSnapshot.member || { id: meta.memberId || '-', name: meta.memberName || '' };
      var loanRecords = Array.isArray(registerSnapshot.loanRecords) ? registerSnapshot.loanRecords : [];
      var totalBalance = 0;
      var historyRowCount = 0;
      var overdueLoanCount = 0;
      for (var lr = 0; lr < loanRecords.length; lr++) {
        var loanRecord = loanRecords[lr] || {};
        totalBalance += Number(loanRecord.loan && loanRecord.loan.balance) || 0;
        var historyItems = Array.isArray(loanRecord.history) ? loanRecord.history : [];
        historyRowCount += historyItems.length;
        if (historyItems.some(function(item) { return !!(item && item.note === 'ขาดส่ง'); })) overdueLoanCount += 1;
      }
      appendReportCoverPage_(body, 'รายงานทะเบียนคุม กลุ่มออมทรัพย์เพื่อการผลิตบ้านพิตำ', 'สมาชิก ' + String(member.id || '-') + ' ' + String(member.name || ''), [
        ['ข้อมูล ณ วันที่', String(meta.reportDateDisplay || '')],
        ['จำนวนสัญญา', formatNumberForDisplay_(loanRecords.length)],
        ['สัญญาที่มีขาดส่ง', formatNumberForDisplay_(overdueLoanCount)],
        ['ยอดหนี้คงเหลือรวม', formatPdfMoneyValue_(totalBalance)],
        ['รายการเคลื่อนไหวในตาราง', formatNumberForDisplay_(historyRowCount)]
      ]);
      if (!loanRecords.length) {
        body.appendParagraph('ไม่พบข้อมูลรายงานทะเบียนคุม');
      } else {
        body.appendPageBreak();
        var globalPage = 0;
        for (var rc = 0; rc < loanRecords.length; rc++) {
          var record = loanRecords[rc] || {};
          var history = Array.isArray(record.history) ? record.history : [];
          var registerPages = buildPagedRowsForPdf_(history, rowsPerPage, function() {
            return { date: '', principal: 0, interest: 0, balance: 0, note: '', isBlank: true };
          });
          for (var rp = 0; rp < registerPages.length; rp++) {
            var registerRows = registerPages[rp];
            var regTotals = { principal: 0, interest: 0, balance: 0 };
            var registerTableRows = [];
            for (var rr = 0; rr < registerRows.length; rr++) {
              var rRow = registerRows[rr] || {};
              var isBlank = !rRow.date && !rRow.note && !(Number(rRow.principal) || Number(rRow.interest) || Number(rRow.balance));
              if (!isBlank) {
                regTotals.principal += Number(rRow.principal) || 0;
                regTotals.interest += Number(rRow.interest) || 0;
                regTotals.balance += Number(rRow.balance) || 0;
              }
              registerTableRows.push([
                isBlank ? '' : ((rp * rowsPerPage) + rr + 1),
                rRow.date || '',
                isBlank ? '' : formatPdfMoneyValue_(rRow.principal),
                isBlank ? '' : formatPdfMoneyValue_(rRow.interest),
                isBlank ? '' : formatPdfMoneyValue_(rRow.balance),
                rRow.note || ''
              ]);
            }
            globalPage += 1;
            appendReportTablePage_(body, [
              { text: 'รายละเอียดรายงานทะเบียนคุม', bold: true, fontSize: 11 },
              { text: 'สมาชิก ' + String(member.id || '-') + ' ' + String(member.name || '') + ' | สัญญา ' + String(record.loan && record.loan.contract || '-'), fontSize: 9 },
              { text: 'ผู้ค้ำประกัน: ' + String(record.guarantors || '(ไม่มีข้อมูล)') + ' | หน้า ' + (rp + 1) + ' / ' + registerPages.length, fontSize: 8, alignment: DocumentApp.HorizontalAlignment.RIGHT }
            ], ['ลำดับ', 'ด/ด/ป (พ.ศ.)', 'ชำระต้น', 'ดอกเบี้ย', 'คงเหลือ', 'หมายเหตุ'], registerTableRows, ['รวมหน้า', '', formatPdfMoneyValue_(regTotals.principal), formatPdfMoneyValue_(regTotals.interest), formatPdfMoneyValue_(regTotals.balance), ''], [DocumentApp.HorizontalAlignment.CENTER, DocumentApp.HorizontalAlignment.CENTER, DocumentApp.HorizontalAlignment.RIGHT, DocumentApp.HorizontalAlignment.RIGHT, DocumentApp.HorizontalAlignment.RIGHT, DocumentApp.HorizontalAlignment.CENTER]);
            if (!(rc === loanRecords.length - 1 && rp === registerPages.length - 1)) body.appendPageBreak();
          }
        }
      }
    } else {
      var overdueSnapshot = snapshot || {};
      var overdueSummary = overdueSnapshot.summary || {};
      var overdueDetails = Array.isArray(overdueSnapshot.details) ? overdueSnapshot.details.slice() : [];
      overdueDetails.sort(compareMonthlyOutstandingRows_);
      for (var od = 0; od < overdueDetails.length; od++) {
        overdueDetails[od].no = od + 1;
        overdueDetails[od].broughtForward = Number(overdueDetails[od].broughtForward != null ? overdueDetails[od].broughtForward : overdueDetails[od].balance) || 0;
      }
      if (!overdueDetails.length) {
        body.appendParagraph('ไม่พบข้อมูลรายงานหนี้คงค้าง');
      } else {
        var overduePages = buildPagedRowsForPdf_(overdueDetails, rowsPerPage, function() {
          return { no: '', memberId: '', nameWithMissed: '', name: '', broughtForward: 0, principalPaid: 0, offsetPaid: 0, interestPaid: 0, balance: 0, note: '', isBlank: true };
        });
        for (var op = 0; op < overduePages.length; op++) {
          var overduePageRows = overduePages[op];
          var overdueTotals = { broughtForward: 0 };
          var overdueTableRows = [];
          for (var or = 0; or < overduePageRows.length; or++) {
            var oRow = overduePageRows[or] || {};
            var emptyRow = !oRow.memberId && !(Number(oRow.broughtForward) || Number(oRow.balance));
            if (!emptyRow) {
              overdueTotals.broughtForward += Number(oRow.broughtForward != null ? oRow.broughtForward : oRow.balance) || 0;
            }
            overdueTableRows.push([
              oRow.no || '',
              oRow.memberId || '',
              oRow.nameWithMissed || oRow.name || '',
              emptyRow ? '' : formatPdfMoneyValue_(oRow.broughtForward != null ? oRow.broughtForward : oRow.balance),
              '',
              '',
              '',
              '',
              ''
            ]);
          }
          appendReportTablePage_(body, [
            { text: 'รายงานหนี้คงค้าง', bold: true, fontSize: 11 },
            { text: 'ประจำเดือน ' + String(meta.periodLabel || ''), fontSize: 9 },
            { text: String(meta.reportDateDisplay || '') ? ('ข้อมูล ณ วันที่ ' + String(meta.reportDateDisplay || '')) : '', fontSize: 8 },
            { text: 'หน้า ' + (op + 1) + ' / ' + overduePages.length + ' | เรียงตามเลขสมาชิก > หนี้คงเหลือ > เลขที่สัญญา', fontSize: 8, alignment: DocumentApp.HorizontalAlignment.RIGHT }
          ], ['ลำดับ', 'รหัสสมาชิก', 'ชื่อ-สกุล', 'หนี้ยกมา', 'ชำระต้น', 'กลบหนี้', 'ดอกเบี้ย', 'คงเหลือ', 'หมายเหตุ'], overdueTableRows, ['รวมหน้า', '', '', formatPdfMoneyValue_(overdueTotals.broughtForward), '', '', '', '', ''], [DocumentApp.HorizontalAlignment.CENTER, DocumentApp.HorizontalAlignment.CENTER, DocumentApp.HorizontalAlignment.LEFT, DocumentApp.HorizontalAlignment.RIGHT, DocumentApp.HorizontalAlignment.RIGHT, DocumentApp.HorizontalAlignment.RIGHT, DocumentApp.HorizontalAlignment.RIGHT, DocumentApp.HorizontalAlignment.RIGHT, DocumentApp.HorizontalAlignment.CENTER]);
          if (op < overduePages.length - 1) body.appendPageBreak();
        }
      }
    }

    doc.saveAndClose();
    var docFile = DriveApp.getFileById(doc.getId());
    var pdfBlob = docFile.getAs(MimeType.PDF);
    pdfBlob.setName(String(meta.fileName || 'financial-report.pdf'));
    return Utilities.base64Encode(pdfBlob.getBytes());
  } finally {
    try { DriveApp.getFileById(doc.getId()).setTrashed(true); } catch (trashDocError) {}
  }
}



/*******************************
 * AutoBackup Google Drive
 * สำรองไฟล์ฐานข้อมูลหลักทุกวันที่ 5 ของเดือน
 * และลบไฟล์ backup เก่ากว่า 90 วันอัตโนมัติ
 *******************************/
var DRIVE_AUTO_BACKUP_TRIGGER_HANDLER_ = 'runScheduledDriveAutoBackup';
var DRIVE_AUTO_BACKUP_HISTORY_SHEET_NAME_ = 'DriveBackupHistory';
var DRIVE_AUTO_BACKUP_DEFAULT_TIMEZONE_ = 'Asia/Bangkok';

function parseDriveAutoBackupBoolean_(value, defaultValue) {
  if (value === '' || value === null || value === undefined) return !!defaultValue;
  if (typeof value === 'boolean') return value;
  var text = String(value || '').trim().toLowerCase();
  if (!text) return !!defaultValue;
  return text === 'true' || text === '1' || text === 'yes' || text === 'on';
}

function sanitizeDriveBackupFilePart_(value) {
  return String(value || '')
    .trim()
    .replace(/[\\/:*?"<>|#%{}~&]+/g, '_')
    .replace(/\s+/g, '_')
    .replace(/_+/g, '_')
    .replace(/^_+|_+$/g, '') || 'BACKUP_DB';
}

function buildDriveFolderUrl_(folderId) {
  var id = String(folderId || '').trim();
  return id ? ('https://drive.google.com/drive/folders/' + id) : '';
}

function buildDriveFileUrl_(fileId) {
  var id = String(fileId || '').trim();
  return id ? ('https://drive.google.com/file/d/' + id + '/view') : '';
}

function ensureDriveBackupHistorySheet_(ss) {
  var spreadsheet = ss || getDatabaseSpreadsheet_();
  var headers = [
    'เวลา',
    'สถานะ',
    'ประเภทการทำงาน',
    'ชื่อไฟล์',
    'File ID',
    'Folder ID',
    'จำนวนไฟล์เก่าที่ลบ',
    'ผู้ดำเนินการ',
    'รายละเอียด'
  ];
  var sheet = ensureSheetWithHeadersSafely_(
    spreadsheet,
    DRIVE_AUTO_BACKUP_HISTORY_SHEET_NAME_,
    headers,
    '#dbeafe',
    'เตรียมชีต DriveBackupHistory'
  );
  setPlainTextColumnFormat_(sheet, [1, 2, 3, 4, 5, 6, 8, 9]);
  if (sheet.getFrozenRows() < 1) sheet.setFrozenRows(1);
  return sheet;
}

function appendDriveBackupHistory_(ss, payload) {
  var spreadsheet = ss || getDatabaseSpreadsheet_();
  var row = [[
    String((payload && payload.timestamp) || getThaiDate(new Date()).dateTime),
    String((payload && payload.status) || '').trim(),
    String((payload && payload.action) || '').trim(),
    String((payload && payload.fileName) || '').trim(),
    String((payload && payload.fileId) || '').trim(),
    String((payload && payload.folderId) || '').trim(),
    Number((payload && payload.cleanedCount) || 0) || 0,
    String((payload && payload.actorName) || '').trim(),
    String((payload && payload.details) || '').trim()
  ]];
  appendRowsSafely_(ensureDriveBackupHistorySheet_(spreadsheet), row, 'บันทึกประวัติ AutoBackup Google Drive');
}

function getDriveAutoBackupSettings_(ss) {
  var spreadsheet = ss || getDatabaseSpreadsheet_();
  var settingsSheet = spreadsheet.getSheetByName('Settings') || createSheetSafely_(spreadsheet, 'Settings', 3, 2, 'เตรียมชีต Settings');

  var runHour = Number(getSettingValue_(settingsSheet, 'DriveAutoBackupRunHour'));
  var runMinute = Number(getSettingValue_(settingsSheet, 'DriveAutoBackupRunMinute'));
  var retentionDays = Number(getSettingValue_(settingsSheet, 'DriveAutoBackupRetentionDays'));

  return {
    enabled: parseDriveAutoBackupBoolean_(getSettingValue_(settingsSheet, 'DriveAutoBackupEnabled'), false),
    folderId: String(getSettingValue_(settingsSheet, 'DriveAutoBackupFolderId') || '').trim(),
    folderName: String(getSettingValue_(settingsSheet, 'DriveAutoBackupFolderName') || '').trim() || ('FinanceWebAppBackup_' + sanitizeDriveBackupFilePart_(spreadsheet.getName())),
    runDayOfMonth: 5,
    runHour: isFinite(runHour) ? Math.max(0, Math.min(23, runHour)) : 1,
    runMinute: isFinite(runMinute) ? Math.max(0, Math.min(59, runMinute)) : 30,
    retentionDays: isFinite(retentionDays) ? Math.max(1, retentionDays) : 90,
    filePrefix: sanitizeDriveBackupFilePart_(String(getSettingValue_(settingsSheet, 'DriveAutoBackupFilePrefix') || '').trim() || ('BACKUP_' + sanitizeDriveBackupFilePart_(spreadsheet.getName()))),
    timezone: String(getSettingValue_(settingsSheet, 'DriveAutoBackupTimezone') || '').trim() || DRIVE_AUTO_BACKUP_DEFAULT_TIMEZONE_,
    deleteOldFiles: parseDriveAutoBackupBoolean_(getSettingValue_(settingsSheet, 'DriveAutoBackupDeleteOldFiles'), true),
    skipIfAlreadyBackedUpToday: parseDriveAutoBackupBoolean_(getSettingValue_(settingsSheet, 'DriveAutoBackupSkipIfAlreadyDoneToday'), true),
    lastSuccessAt: String(getSettingValue_(settingsSheet, 'DriveAutoBackupLastSuccessAt') || '').trim(),
    lastSuccessFileName: String(getSettingValue_(settingsSheet, 'DriveAutoBackupLastSuccessFileName') || '').trim(),
    lastSuccessFileId: String(getSettingValue_(settingsSheet, 'DriveAutoBackupLastSuccessFileId') || '').trim(),
    lastCleanupAt: String(getSettingValue_(settingsSheet, 'DriveAutoBackupLastCleanupAt') || '').trim(),
    lastSuccessDateKey: String(getSettingValue_(settingsSheet, 'DriveAutoBackupLastSuccessDateKey') || '').trim()
  };
}

function getDriveAutoBackupFolder_(ss, settings) {
  var spreadsheet = ss || getDatabaseSpreadsheet_();
  var effectiveSettings = settings || getDriveAutoBackupSettings_(spreadsheet);
  var settingsSheet = spreadsheet.getSheetByName('Settings') || createSheetSafely_(spreadsheet, 'Settings', 3, 2, 'เตรียมชีต Settings');

  if (effectiveSettings.folderId) {
    try {
      return DriveApp.getFolderById(effectiveSettings.folderId);
    } catch (e) {
      // fallback ไป create/find จากชื่อ
    }
  }

  var folderName = String(effectiveSettings.folderName || '').trim() || ('FinanceWebAppBackup_' + sanitizeDriveBackupFilePart_(spreadsheet.getName()));
  var folderIterator = DriveApp.getFoldersByName(folderName);
  var folder = folderIterator.hasNext() ? folderIterator.next() : DriveApp.createFolder(folderName);

  upsertSettingValue_(settingsSheet, 'DriveAutoBackupFolderId', folder.getId());
  upsertSettingValue_(settingsSheet, 'DriveAutoBackupFolderName', folderName);

  return folder;
}

function buildDriveAutoBackupFileName_(ss, settings, dateObj) {
  var spreadsheet = ss || getDatabaseSpreadsheet_();
  var effectiveSettings = settings || getDriveAutoBackupSettings_(spreadsheet);
  var d = dateObj || new Date();
  var yearBe = d.getFullYear() + 543;
  var month = padNumber_(d.getMonth() + 1, 2);
  var day = padNumber_(d.getDate(), 2);
  var hh = padNumber_(d.getHours(), 2);
  var mm = padNumber_(d.getMinutes(), 2);
  var prefix = sanitizeDriveBackupFilePart_(effectiveSettings.filePrefix || ('BACKUP_' + sanitizeDriveBackupFilePart_(spreadsheet.getName())));
  return prefix + '_' + yearBe + '-' + month + '-' + day + '_' + hh + mm;
}

function getDriveAutoBackupHistory(filterStr) {
  requirePermission_('settings.view', 'ไม่มีสิทธิ์ดูประวัติ AutoBackup Google Drive');
  try {
    var filter = typeof filterStr === 'string' ? JSON.parse(filterStr || '{}') : (filterStr || {});
    var limit = Math.max(1, Math.min(300, Number(filter.limit) || 60));
    var statusFilter = String(filter.status || 'ALL').trim().toUpperCase();
    var cacheKey = 'driveAutoBackupHistory::' + limit + '::' + (statusFilter || 'ALL');
    var cached = getJsonCache_(cacheKey);
    if (cached && Array.isArray(cached.records)) {
      return { status: 'Success', records: cached.records, cached: true };
    }

    var sheet = ensureDriveBackupHistorySheet_(getDatabaseSpreadsheet_());
    if (sheet.getLastRow() <= 1) return { status: 'Success', records: [] };

    var values = readSheetRowsFromBottomFiltered_(
      sheet,
      2,
      9,
      limit,
      statusFilter === 'ALL' ? null : function(row) {
        return String(row[1] || '').trim().toUpperCase() === statusFilter;
      },
      200
    );

    var rows = values.map(function(row) {
      var rowStatus = String(row[1] || '').trim().toUpperCase();
      var fileId = String(row[4] || '').trim();
      var folderId = String(row[5] || '').trim();
      return {
        timestamp: String(row[0] || '').trim(),
        status: rowStatus,
        action: String(row[2] || '').trim(),
        fileName: String(row[3] || '').trim(),
        fileId: fileId,
        fileUrl: buildDriveFileUrl_(fileId),
        folderId: folderId,
        folderUrl: buildDriveFolderUrl_(folderId),
        cleanedCount: Number(row[6]) || 0,
        actorName: String(row[7] || '').trim(),
        details: String(row[8] || '').trim()
      };
    });

    putJsonCache_(cacheKey, { records: rows }, 30);
    return { status: 'Success', records: rows, cached: false };
  } catch (e) {
    return { status: 'Error', message: 'โหลดประวัติ AutoBackup Google Drive ไม่สำเร็จ: ' + e.toString(), records: [] };
  }
}

function getDriveAutoBackupTriggerStatus_() {
  var result = {
    installed: false,
    count: 0,
    handler: DRIVE_AUTO_BACKUP_TRIGGER_HANDLER_,
    scheduleLabel: 'ทุกวันที่ 5 เวลา 01:30 (Asia/Bangkok)',
    error: ''
  };

  try {
    var triggers = ScriptApp.getProjectTriggers();
    for (var i = 0; i < triggers.length; i++) {
      if (String(triggers[i].getHandlerFunction() || '') === DRIVE_AUTO_BACKUP_TRIGGER_HANDLER_) {
        result.count++;
      }
    }
    result.installed = result.count > 0;
  } catch (e) {
    result.error = e.toString();
  }

  return result;
}

function removeDriveAutoBackupTriggers_() {
  var removed = 0;
  var triggers = ScriptApp.getProjectTriggers();
  for (var i = 0; i < triggers.length; i++) {
    if (String(triggers[i].getHandlerFunction() || '') !== DRIVE_AUTO_BACKUP_TRIGGER_HANDLER_) continue;
    ScriptApp.deleteTrigger(triggers[i]);
    removed++;
  }
  return removed;
}

function getDriveAutoBackupSettings() {
  requirePermission_('settings.view', 'ไม่มีสิทธิ์ดูการตั้งค่า AutoBackup');
  try {
    var cacheKey = 'driveAutoBackupSettingsCache';
    var cached = getJsonCache_(cacheKey);
    if (cached && cached.settings) {
      return {
        status: 'Success',
        settings: cached.settings,
        trigger: cached.trigger || { installed: false, count: 0, error: '' },
        cached: true
      };
    }
    var ss = getDatabaseSpreadsheet_();
    var settings = getDriveAutoBackupSettings_(ss);
    var trigger = getDriveAutoBackupTriggerStatus_();
    putJsonCache_(cacheKey, { settings: settings, trigger: trigger }, 60);
    return {
      status: 'Success',
      settings: settings,
      trigger: trigger,
      cached: false
    };
  } catch (e) {
    return {
      status: 'Error',
      message: 'โหลดการตั้งค่า AutoBackup ไม่สำเร็จ: ' + e.toString()
    };
  }
}

function saveDriveAutoBackupSettings(payloadStr) {
  requirePermission_('settings.manage', 'ไม่มีสิทธิ์แก้ไขการตั้งค่า AutoBackup');

  try {
    var payload = typeof payloadStr === 'string' ? JSON.parse(payloadStr || '{}') : (payloadStr || {});
    var ss = getDatabaseSpreadsheet_();
    var settingsSheet = ss.getSheetByName('Settings') || createSheetSafely_(ss, 'Settings', 3, 2, 'เตรียมชีต Settings');

    var folderId = String(payload.folderId || '').trim();
    var folderName = String(payload.folderName || '').trim();
    if (folderId) {
      try {
        DriveApp.getFolderById(folderId);
      } catch (e) {
        return { status: 'Error', message: 'Folder ID ที่ระบุไม่ถูกต้อง หรือบัญชีนี้ไม่มีสิทธิ์เข้าถึงโฟลเดอร์ดังกล่าว' };
      }
    }

    upsertSettingValue_(settingsSheet, 'DriveAutoBackupEnabled', !!payload.enabled);
    upsertSettingValue_(settingsSheet, 'DriveAutoBackupFolderId', folderId);
    upsertSettingValue_(settingsSheet, 'DriveAutoBackupFolderName', folderName || ('FinanceWebAppBackup_' + sanitizeDriveBackupFilePart_(ss.getName())));
    upsertSettingValue_(settingsSheet, 'DriveAutoBackupRunHour', Math.max(0, Math.min(23, Number(payload.runHour) || 1)));
    upsertSettingValue_(settingsSheet, 'DriveAutoBackupRunMinute', Math.max(0, Math.min(59, Number(payload.runMinute) || 30)));
    upsertSettingValue_(settingsSheet, 'DriveAutoBackupRetentionDays', Math.max(1, Number(payload.retentionDays) || 90));
    upsertSettingValue_(settingsSheet, 'DriveAutoBackupFilePrefix', sanitizeDriveBackupFilePart_(payload.filePrefix || ('BACKUP_' + sanitizeDriveBackupFilePart_(ss.getName()))));
    upsertSettingValue_(settingsSheet, 'DriveAutoBackupTimezone', String(payload.timezone || DRIVE_AUTO_BACKUP_DEFAULT_TIMEZONE_).trim() || DRIVE_AUTO_BACKUP_DEFAULT_TIMEZONE_);
    upsertSettingValue_(settingsSheet, 'DriveAutoBackupDeleteOldFiles', payload.deleteOldFiles !== false);
    upsertSettingValue_(settingsSheet, 'DriveAutoBackupSkipIfAlreadyDoneToday', payload.skipIfAlreadyBackedUpToday !== false);

    clearAppCache_();

    appendAuditLogSafe_(ss, {
      action: 'SAVE_DRIVE_AUTO_BACKUP_SETTINGS',
      entityType: 'SYSTEM',
      entityId: 'DriveAutoBackup',
      after: getDriveAutoBackupSettings_(ss),
      details: 'อัปเดตการตั้งค่า AutoBackup ไป Google Drive'
    });
    logSystemActionFast(ss, 'ตั้งค่า AutoBackup Google Drive', 'บันทึกการตั้งค่า AutoBackup เรียบร้อย');

    return {
      status: 'Success',
      message: 'บันทึกการตั้งค่า AutoBackup เรียบร้อยแล้ว',
      settings: getDriveAutoBackupSettings_(ss),
      trigger: getDriveAutoBackupTriggerStatus_()
    };
  } catch (e) {
    return { status: 'Error', message: 'บันทึกการตั้งค่า AutoBackup ไม่สำเร็จ: ' + e.toString() };
  }
}

function installDriveAutoBackupTrigger() {
  requirePermission_('settings.manage', 'ไม่มีสิทธิ์ติดตั้ง trigger AutoBackup');

  try {
    var ss = getDatabaseSpreadsheet_();
    var settings = getDriveAutoBackupSettings_(ss);
    var removedCount = removeDriveAutoBackupTriggers_();

    ScriptApp.newTrigger(DRIVE_AUTO_BACKUP_TRIGGER_HANDLER_)
      .timeBased()
      .onMonthDay(5)
      .atHour(settings.runHour)
      .nearMinute(settings.runMinute)
      .inTimezone(settings.timezone || DRIVE_AUTO_BACKUP_DEFAULT_TIMEZONE_)
      .create();

    var settingsSheet = ss.getSheetByName('Settings') || createSheetSafely_(ss, 'Settings', 3, 2, 'เตรียมชีต Settings');
    upsertSettingValue_(settingsSheet, 'DriveAutoBackupTriggerInstalledAt', getThaiDate(new Date()).dateTime);

    logSystemActionFast(
      ss,
      'ติดตั้ง Trigger AutoBackup',
      'ติดตั้ง trigger สำรองข้อมูลอัตโนมัติทุกวันที่ 5 | ลบ trigger เดิม ' + removedCount + ' รายการ'
    );

    return {
      status: 'Success',
      message: 'ติดตั้ง trigger AutoBackup สำเร็จ',
      removedCount: removedCount,
      trigger: getDriveAutoBackupTriggerStatus_()
    };
  } catch (e) {
    return { status: 'Error', message: 'ติดตั้ง trigger AutoBackup ไม่สำเร็จ: ' + e.toString() };
  }
}

function removeDriveAutoBackupTrigger() {
  requirePermission_('settings.manage', 'ไม่มีสิทธิ์ปิด trigger AutoBackup');

  try {
    var ss = getDatabaseSpreadsheet_();
    var removedCount = removeDriveAutoBackupTriggers_();
    logSystemActionFast(ss, 'ปิด Trigger AutoBackup', 'ลบ trigger AutoBackup ออก ' + removedCount + ' รายการ');
    return {
      status: 'Success',
      message: removedCount > 0 ? 'ปิด trigger AutoBackup เรียบร้อยแล้ว' : 'ไม่พบ trigger AutoBackup ที่ต้องลบ',
      removedCount: removedCount,
      trigger: getDriveAutoBackupTriggerStatus_()
    };
  } catch (e) {
    return { status: 'Error', message: 'ปิด trigger AutoBackup ไม่สำเร็จ: ' + e.toString() };
  }
}

function cleanupOldDriveBackupFiles_(folder, settings, now) {
  var deletedCount = 0;
  var checkedCount = 0;
  var cutoff = new Date((now || new Date()).getTime() - ((Number(settings.retentionDays) || 90) * 24 * 60 * 60 * 1000));
  var files = folder.getFiles();
  var prefix = String(settings.filePrefix || '').trim();

  while (files.hasNext()) {
    var file = files.next();
    checkedCount++;

    var fileName = String(file.getName() || '').trim();
    if (prefix && fileName.indexOf(prefix) !== 0) continue;

    var createdAt = file.getDateCreated();
    if (!createdAt || createdAt.getTime() >= cutoff.getTime()) continue;

    try {
      if (typeof file.isTrashed === 'function' && file.isTrashed()) continue;
    } catch (ignored) {}

    file.setTrashed(true);
    deletedCount++;
  }

  return {
    checkedCount: checkedCount,
    deletedCount: deletedCount,
    cutoffDate: cutoff
  };
}

function executeDriveAutoBackup_(options) {
  var opt = options || {};
  var lock = LockService.getScriptLock();
  var locked = false;

  try {
    lock.waitLock(30000);
    locked = true;

    var ss = getDatabaseSpreadsheet_();
    var settingsSheet = ss.getSheetByName('Settings') || createSheetSafely_(ss, 'Settings', 3, 2, 'เตรียมชีต Settings');
    var settings = getDriveAutoBackupSettings_(ss);

    if (!settings.enabled && !opt.manual) {
      return {
        status: 'Success',
        skipped: true,
        message: 'AutoBackup ถูกปิดใช้งานอยู่'
      };
    }

    var now = new Date();
    var timezone = settings.timezone || DRIVE_AUTO_BACKUP_DEFAULT_TIMEZONE_;
    var runDateKey = Utilities.formatDate(now, timezone, 'yyyy-MM-dd');

    if (!opt.manual && settings.skipIfAlreadyBackedUpToday && String(settings.lastSuccessDateKey || '') === runDateKey) {
      return {
        status: 'Success',
        skipped: true,
        message: 'วันนี้ระบบสำรองข้อมูลเรียบร้อยแล้ว'
      };
    }

    var session = opt.manual ? (getAuthenticatedSession_() || {}) : {};
    var actorName = opt.actorName || (opt.manual ? (session.fullName || session.username || 'Admin') : 'ระบบ AutoBackup');
    var actorUsername = opt.actorUsername || (opt.manual ? (session.username || 'admin') : 'system');
    var actorRole = opt.actorRole || (opt.manual ? (session.role || 'admin') : 'system');

    var targetFolder = getDriveAutoBackupFolder_(ss, settings);
    var sourceFile = DriveApp.getFileById(ss.getId());
    var backupFileName = buildDriveAutoBackupFileName_(ss, settings, now);
    var backupFile = sourceFile.makeCopy(backupFileName, targetFolder);

    try {
      backupFile.setDescription(
        'AutoBackup of spreadsheet ' + ss.getId() +
        ' | Source=' + ss.getName() +
        ' | GeneratedAt=' + getThaiDate(now).dateTime
      );
    } catch (ignored) {}

    var cleanup = { checkedCount: 0, deletedCount: 0, cutoffDate: null };
    if (settings.deleteOldFiles) {
      cleanup = cleanupOldDriveBackupFiles_(targetFolder, settings, now);
      upsertSettingValue_(settingsSheet, 'DriveAutoBackupLastCleanupAt', getThaiDate(now).dateTime);
    }

    upsertSettingValue_(settingsSheet, 'DriveAutoBackupLastSuccessAt', getThaiDate(now).dateTime);
    upsertSettingValue_(settingsSheet, 'DriveAutoBackupLastSuccessFileName', backupFileName);
    upsertSettingValue_(settingsSheet, 'DriveAutoBackupLastSuccessFileId', backupFile.getId());
    upsertSettingValue_(settingsSheet, 'DriveAutoBackupLastSuccessDateKey', runDateKey);

    appendDriveBackupHistory_(ss, {
      timestamp: getThaiDate(now).dateTime,
      status: 'SUCCESS',
      action: opt.manual ? 'MANUAL_DRIVE_AUTO_BACKUP' : 'SCHEDULED_DRIVE_AUTO_BACKUP',
      fileName: backupFileName,
      fileId: backupFile.getId(),
      folderId: targetFolder.getId(),
      cleanedCount: cleanup.deletedCount || 0,
      actorName: actorName,
      details: 'สำรองไฟล์ฐานข้อมูลหลักไป Google Drive สำเร็จ'
    });

    appendAuditLogSafe_(ss, {
      username: actorUsername,
      actorName: actorName,
      role: actorRole,
      action: 'DRIVE_AUTO_BACKUP',
      entityType: 'SYSTEM',
      entityId: 'DriveAutoBackup',
      referenceNo: backupFile.getId(),
      after: {
        fileName: backupFileName,
        fileId: backupFile.getId(),
        folderId: targetFolder.getId(),
        cleanedCount: cleanup.deletedCount || 0,
        retentionDays: settings.retentionDays
      },
      details: 'สำรองไฟล์ฐานข้อมูลหลักไป Google Drive สำเร็จ'
    });

    logSystemActionFast(
      ss,
      opt.manual ? 'สำรองฐานข้อมูลไป Google Drive (Manual)' : 'สำรองฐานข้อมูลไป Google Drive (Auto)',
      'ไฟล์: ' + backupFileName + ' | ลบไฟล์เก่า: ' + (cleanup.deletedCount || 0) + ' ไฟล์'
    );

    sendNotificationEmailIfEnabled_(ss, 'notifyBackupCompleted', 'แจ้งเตือน: สำรองฐานข้อมูลไป Google Drive สำเร็จ', [
      'ระบบสำรองฐานข้อมูลไป Google Drive สำเร็จ',
      'ไฟล์: ' + backupFileName,
      'File ID: ' + backupFile.getId(),
      'Folder ID: ' + targetFolder.getId(),
      'ลบไฟล์เก่า: ' + (cleanup.deletedCount || 0) + ' ไฟล์',
      'เวลา: ' + getThaiDate(now).dateTime,
      'รูปแบบ: สำเนาไฟล์ Spreadsheet ทั้งไฟล์'
    ]);

    clearAppCache_();

    return {
      status: 'Success',
      fileName: backupFileName,
      fileId: backupFile.getId(),
      fileUrl: buildDriveFileUrl_(backupFile.getId()),
      folderId: targetFolder.getId(),
      folderUrl: buildDriveFolderUrl_(targetFolder.getId()),
      deletedOldFiles: cleanup.deletedCount || 0,
      message: 'สำรองฐานข้อมูลไป Google Drive เรียบร้อยแล้ว'
    };
  } catch (e) {
    try {
      var ssErr = getDatabaseSpreadsheet_();
      appendDriveBackupHistory_(ssErr, {
        timestamp: getThaiDate(new Date()).dateTime,
        status: 'ERROR',
        action: opt.manual ? 'MANUAL_DRIVE_AUTO_BACKUP' : 'SCHEDULED_DRIVE_AUTO_BACKUP',
        fileName: '',
        fileId: '',
        folderId: '',
        cleanedCount: 0,
        actorName: opt.actorName || (opt.manual ? 'Admin' : 'ระบบ AutoBackup'),
        details: e.toString()
      });
      logSystemActionFast(ssErr, 'AutoBackup Google Drive ล้มเหลว', e.toString());
    } catch (ignored2) {}

    return { status: 'Error', message: 'สำรองฐานข้อมูลไป Google Drive ไม่สำเร็จ: ' + e.toString() };
  } finally {
    if (locked) lock.releaseLock();
  }
}

function runDriveAutoBackupNow() {
  requirePermission_('settings.manage', 'ไม่มีสิทธิ์ทดสอบสำรองข้อมูล');
  var session = getAuthenticatedSession_() || {};
  return executeDriveAutoBackup_({
    manual: true,
    actorName: session.fullName || session.username || 'Admin',
    actorUsername: session.username || 'admin',
    actorRole: session.role || 'admin'
  });
}

function runScheduledDriveAutoBackup() {
  return executeDriveAutoBackup_({
    manual: false,
    actorName: 'ระบบ AutoBackup',
    actorUsername: 'system',
    actorRole: 'system'
  });
}


// ==========================================
// PHASE 18: ENTITY TIMELINE (Member / Loan)
// ==========================================
function normalizeEntityTimelineDateText_(value) {
  return String(value || '').trim();
}

function buildEntityTimelineSortKey_(dateText, fallbackWeight) {
  var text = normalizeEntityTimelineDateText_(dateText);
  var dateOnly = text ? String(text).split(' ')[0] : '';
  var dateNumber = getThaiDateNumber_(dateOnly);
  var timePart = text.indexOf(' ') !== -1 ? text.substring(text.indexOf(' ') + 1).trim() : '';
  var timeDigits = timePart.replace(/[^0-9]/g, '').substring(0, 6);
  while (timeDigits.length < 6) timeDigits += '0';
  var base = String(dateNumber || 0) + timeDigits;
  return base + ':' + padNumber_(Math.max(0, Number(fallbackWeight) || 0), 6);
}

function buildEntityTimelineEvent_(payload) {
  var event = payload || {};
  return {
    eventId: String(event.eventId || Utilities.getUuid()).trim(),
    entityType: String(event.entityType || '').trim(),
    entityId: String(event.entityId || '').trim(),
    happenedAt: normalizeEntityTimelineDateText_(event.happenedAt),
    sortKey: String(event.sortKey || buildEntityTimelineSortKey_(event.happenedAt, event.fallbackWeight)).trim(),
    category: String(event.category || 'ทั่วไป').trim(),
    categoryKey: String(event.categoryKey || 'general').trim(),
    title: String(event.title || '-').trim(),
    detail: String(event.detail || '').trim(),
    contract: String(event.contract || '').trim(),
    memberId: String(event.memberId || '').trim(),
    actorName: String(event.actorName || '').trim(),
    amountLabel: String(event.amountLabel || '').trim(),
    statusLabel: String(event.statusLabel || '').trim(),
    tone: String(event.tone || 'slate').trim(),
    sourceRef: String(event.sourceRef || '').trim(),
    fileUrl: String(event.fileUrl || '').trim(),
    fileName: String(event.fileName || '').trim()
  };
}

function sortEntityTimelineEventsDesc_(events) {
  var rows = Array.isArray(events) ? events.slice() : [];
  rows.sort(function(a, b) {
    var keyA = String((a && a.sortKey) || '');
    var keyB = String((b && b.sortKey) || '');
    if (keyA !== keyB) return keyB.localeCompare(keyA);
    return String((b && b.eventId) || '').localeCompare(String((a && a.eventId) || ''));
  });
  return rows;
}

function summarizeEntityTimeline_(events) {
  var rows = Array.isArray(events) ? events : [];
  var summary = {
    totalEvents: rows.length,
    transactionEvents: 0,
    noteEvents: 0,
    attachmentEvents: 0,
    createdEvents: 0,
    relationEvents: 0,
    latestAt: rows.length ? String(rows[0].happenedAt || '') : ''
  };
  for (var i = 0; i < rows.length; i++) {
    var key = String(rows[i].categoryKey || '').trim();
    if (key === 'transaction') summary.transactionEvents += 1;
    else if (key === 'note') summary.noteEvents += 1;
    else if (key === 'attachment') summary.attachmentEvents += 1;
    else if (key === 'created') summary.createdEvents += 1;
    else if (key === 'relation') summary.relationEvents += 1;
  }
  return summary;
}

function buildLoanCreationTimelineEvent_(loan, ownerEntityType, ownerEntityId, mode) {
  if (!loan) return null;
  var createdAt = normalizeEntityTimelineDateText_(loan.createdAt);
  if (!createdAt) return null;
  var contract = String(loan.contract || '').trim();
  var memberId = String(loan.memberId || '').trim();
  var memberName = String(loan.member || '').trim();
  var detail = mode === 'guarantor'
    ? ('เป็นผู้ค้ำประกันสัญญา ' + contract + ' ของ ' + (memberName || '-') + ' | วงเงิน ' + formatNumberForDisplay_(loan.amount || 0) + ' | คงเหลือ ' + formatNumberForDisplay_(loan.balance || 0))
    : ('เปิดสัญญา ' + contract + ' | วงเงิน ' + formatNumberForDisplay_(loan.amount || 0) + ' | อัตราดอกเบี้ย ' + formatNumberForDisplay_(loan.interest || 0) + '% | คงเหลือ ' + formatNumberForDisplay_(loan.balance || 0));
  return buildEntityTimelineEvent_({
    eventId: 'TL-LOAN-' + contract + '-' + mode,
    entityType: ownerEntityType,
    entityId: ownerEntityId,
    happenedAt: createdAt,
    category: mode === 'guarantor' ? 'ความเกี่ยวข้อง' : 'เปิดสัญญา',
    categoryKey: mode === 'guarantor' ? 'relation' : 'created',
    title: mode === 'guarantor' ? ('เป็นผู้ค้ำประกัน ' + contract) : ('สร้างสัญญา ' + contract),
    detail: detail,
    contract: contract,
    memberId: memberId,
    statusLabel: String(loan.status || '').trim(),
    tone: mode === 'guarantor' ? 'indigo' : 'emerald',
    sourceRef: contract,
    fallbackWeight: mode === 'guarantor' ? 400000 : 300000
  });
}

function buildTransactionTimelineEvents_(transactions, entityType, entityId, compactMode) {
  var rows = Array.isArray(transactions) ? transactions : [];
  var events = [];
  for (var i = 0; i < rows.length; i++) {
    var tx = rows[i] || {};
    var statusLabel = String(tx.txStatus || 'ปกติ').trim() || 'ปกติ';
    var noteText = String(tx.note || '').trim();
    var category = 'รับชำระ';
    var tone = 'emerald';
    if (statusLabel === 'กลับรายการ' || statusLabel === 'ยกเลิก') {
      category = statusLabel;
      tone = 'rose';
    } else if (noteText.indexOf('กลบหนี้') !== -1) {
      category = 'กลบหนี้';
      tone = 'amber';
    }
    var amountParts = [];
    if ((Number(tx.principalPaid) || 0) !== 0) amountParts.push('ต้น ' + formatNumberForDisplay_(tx.principalPaid || 0));
    if ((Number(tx.interestPaid) || 0) !== 0) amountParts.push('ดอก ' + formatNumberForDisplay_(tx.interestPaid || 0));
    amountParts.push('คงเหลือ ' + formatNumberForDisplay_(tx.newBalance || 0));
    var detailParts = [];
    if (!compactMode && tx.contract) detailParts.push('สัญญา ' + String(tx.contract || '').trim());
    detailParts.push(amountParts.join(' | '));
    if (noteText) detailParts.push('หมายเหตุ ' + noteText);
    events.push(buildEntityTimelineEvent_({
      eventId: 'TL-TX-' + String(tx.id || i + 1).trim(),
      entityType: entityType,
      entityId: entityId,
      happenedAt: String(tx.timestamp || '').trim() || String(tx.dateOnly || '').trim(),
      category: category,
      categoryKey: 'transaction',
      title: compactMode ? (category + ' สัญญา ' + String(tx.contract || '-').trim()) : category,
      detail: detailParts.join(' | '),
      contract: String(tx.contract || '').trim(),
      memberId: String(tx.memberId || '').trim(),
      amountLabel: amountParts.join(' | '),
      statusLabel: statusLabel,
      tone: tone,
      sourceRef: String(tx.id || '').trim(),
      fallbackWeight: 200000 + i
    }));
  }
  return events;
}

function buildNoteTimelineEvents_(notes, entityType, entityId) {
  var rows = Array.isArray(notes) ? notes : [];
  var events = [];
  for (var i = 0; i < rows.length; i++) {
    var note = rows[i] || {};
    var noteText = String(note.noteText || '').trim();
    events.push(buildEntityTimelineEvent_({
      eventId: 'TL-NOTE-' + String(note.noteId || i + 1).trim(),
      entityType: entityType,
      entityId: entityId,
      happenedAt: String(note.createdAt || '').trim(),
      category: 'หมายเหตุภายใน',
      categoryKey: 'note',
      title: 'บันทึกหมายเหตุ',
      detail: noteText,
      actorName: String(note.createdByName || note.createdBy || '').trim(),
      tone: 'emerald',
      sourceRef: String(note.noteId || '').trim(),
      fallbackWeight: 500000 + i
    }));
  }
  return events;
}

function buildAttachmentTimelineEvents_(attachments, entityType, entityId) {
  var rows = Array.isArray(attachments) ? attachments : [];
  var events = [];
  for (var i = 0; i < rows.length; i++) {
    var attachment = rows[i] || {};
    var detailParts = ['ไฟล์ ' + String(attachment.fileName || '-').trim()];
    if (String(attachment.note || '').trim()) detailParts.push('หมายเหตุ ' + String(attachment.note || '').trim());
    events.push(buildEntityTimelineEvent_({
      eventId: 'TL-ATT-' + String(attachment.attachmentId || i + 1).trim(),
      entityType: entityType,
      entityId: entityId,
      happenedAt: String(attachment.createdAt || '').trim(),
      category: 'ไฟล์แนบ',
      categoryKey: 'attachment',
      title: 'เพิ่มไฟล์แนบ',
      detail: detailParts.join(' | '),
      actorName: String(attachment.createdByName || attachment.createdBy || '').trim(),
      tone: 'amber',
      sourceRef: String(attachment.attachmentId || '').trim(),
      fileUrl: String(attachment.fileUrl || '').trim(),
      fileName: String(attachment.fileName || '').trim(),
      fallbackWeight: 600000 + i
    }));
  }
  return events;
}

function buildMemberTimelinePayload_(result) {
  var member = (result && result.member) || {};
  var memberId = String(member.id || '').trim();
  var events = [];
  var myLoans = (result && result.myLoans) || [];
  var guaranteedLoans = (result && result.guaranteedLoans) || [];
  for (var i = 0; i < myLoans.length; i++) {
    var ownEvent = buildLoanCreationTimelineEvent_(myLoans[i], 'MEMBER', memberId, 'owner');
    if (ownEvent) events.push(ownEvent);
  }
  for (var g = 0; g < guaranteedLoans.length; g++) {
    var guarantorEvent = buildLoanCreationTimelineEvent_(guaranteedLoans[g], 'MEMBER', memberId, 'guarantor');
    if (guarantorEvent) events.push(guarantorEvent);
  }
  events = events.concat(buildTransactionTimelineEvents_((result && result.transactions) || [], 'MEMBER', memberId, true));
  events = events.concat(buildNoteTimelineEvents_((result && result.notes) || [], 'MEMBER', memberId));
  events = events.concat(buildAttachmentTimelineEvents_((result && result.attachments) || [], 'MEMBER', memberId));
  events = sortEntityTimelineEventsDesc_(events).slice(0, 120);
  return {
    events: events,
    summary: summarizeEntityTimeline_(events)
  };
}

function buildLoanTimelinePayload_(result) {
  var loan = (result && result.loan) || {};
  var contract = String(loan.contract || '').trim();
  var events = [];
  var createdEvent = buildLoanCreationTimelineEvent_(loan, 'LOAN', contract, 'owner');
  if (createdEvent) {
    createdEvent.title = 'สร้างสัญญา';
    createdEvent.detail = ['สัญญา ' + contract, 'วงเงิน ' + formatNumberForDisplay_(loan.amount || 0), 'ดอกเบี้ย ' + formatNumberForDisplay_(loan.interest || 0) + '%', 'คงเหลือ ' + formatNumberForDisplay_(loan.balance || 0)].join(' | ');
    events.push(createdEvent);
  }
  events = events.concat(buildTransactionTimelineEvents_((result && result.transactions) || [], 'LOAN', contract, false));
  events = events.concat(buildNoteTimelineEvents_((result && result.notes) || [], 'LOAN', contract));
  events = events.concat(buildAttachmentTimelineEvents_((result && result.attachments) || [], 'LOAN', contract));
  events = sortEntityTimelineEventsDesc_(events).slice(0, 150);
  return {
    events: events,
    summary: summarizeEntityTimeline_(events)
  };
}

var __phase18_orig_getMemberDetail = getMemberDetail;
getMemberDetail = function(memberId) {
  var result = __phase18_orig_getMemberDetail(memberId);
  if (!result || result.status === 'Error') return result;
  try {
    var timelinePayload = buildMemberTimelinePayload_(result);
    result.timeline = timelinePayload.events;
    result.timelineSummary = timelinePayload.summary;
  } catch (e) {
    result.timeline = [];
    result.timelineSummary = summarizeEntityTimeline_([]);
  }
  return result;
};

var __phase18_orig_getLoanDetail = getLoanDetail;
getLoanDetail = function(contractNo) {
  var result = __phase18_orig_getLoanDetail(contractNo);
  if (!result || result.status === 'Error') return result;
  try {
    var timelinePayload = buildLoanTimelinePayload_(result);
    result.timeline = timelinePayload.events;
    result.timelineSummary = timelinePayload.summary;
  } catch (e) {
    result.timeline = [];
    result.timelineSummary = summarizeEntityTimeline_([]);
  }
  return result;
};


// PHASE 19: QUICK OVERVIEW FOR MEMBER / LOAN DETAIL
function getQuickOverviewTimestampValue_(candidate) {
  if (candidate === null || candidate === undefined || candidate === '') return '';
  return String(candidate || '').trim();
}

function pickLatestRecordByFields_(items, fieldNames) {
  var safeItems = Array.isArray(items) ? items : [];
  var fields = Array.isArray(fieldNames) ? fieldNames : [];
  var best = null;
  var bestScore = -1;
  var bestText = '';
  for (var i = 0; i < safeItems.length; i++) {
    var item = safeItems[i] || {};
    var text = '';
    for (var f = 0; f < fields.length; f++) {
      var raw = getQuickOverviewTimestampValue_(item[fields[f]]);
      if (raw) {
        text = raw;
        break;
      }
    }
    var dateOnly = text ? text.split(' ')[0].trim() : '';
    var score = dateOnly ? getThaiDateNumber_(dateOnly) : 0;
    if (score > bestScore || (score === bestScore && text > bestText)) {
      best = item;
      bestScore = score;
      bestText = text;
    }
  }
  return best;
}

function getCurrentThaiYearBe_() {
  var ctx = getCurrentThaiMonthContext_();
  return Number((ctx && ctx.yearBe) || (new Date().getFullYear() + 543)) || (new Date().getFullYear() + 543);
}

function getPaidInstallmentsYtdFromTransactions_(transactions, targetYearBe) {
  var safeRows = Array.isArray(transactions) ? transactions : [];
  var yearBe = Number(targetYearBe) || getCurrentThaiYearBe_();
  var total = 0;
  for (var i = 0; i < safeRows.length; i++) {
    var tx = safeRows[i] || {};
    if (String(tx.txStatus || '').trim() !== 'ปกติ') continue;
    var parts = parseThaiDateParts_(tx.timestamp || tx.dateOnly || '');
    if (!parts || Number(parts.yearBe) !== yearBe) continue;
    total += Math.max(0, Number(tx.interestMonthsPaid) || 0);
  }
  return total;
}

function buildOverviewCard_(label, value, subValue, tone, actionLabel, actionType, actionValue) {
  return {
    label: String(label || '').trim(),
    value: String(value || '-').trim() || '-',
    subValue: String(subValue || '').trim(),
    tone: String(tone || 'slate').trim() || 'slate',
    actionLabel: String(actionLabel || '').trim(),
    actionType: String(actionType || '').trim(),
    actionValue: String(actionValue || '').trim()
  };
}

function buildMemberQuickOverview_(ss, result) {
  var spreadsheet = ss || getDatabaseSpreadsheet_();
  var detail = result || {};
  var member = detail.member || {};
  var memberId = String(member.id || '').trim();
  var myLoans = Array.isArray(detail.myLoans) ? detail.myLoans : [];
  var notes = Array.isArray(detail.notes) ? detail.notes : [];
  var attachments = Array.isArray(detail.attachments) ? detail.attachments : [];
  var transactions = Array.isArray(detail.transactions) ? detail.transactions : [];
  var latestTx = pickLatestRecordByFields_(transactions, ['timestamp', 'dateOnly']);
  var latestNote = pickLatestRecordByFields_(notes, ['updatedAt', 'createdAt']);
  var latestAttachment = pickLatestRecordByFields_(attachments, ['updatedAt', 'createdAt']);
  var highestOutstandingLoan = null;
  var highestOutstandingValue = -1;
  var overdueLoanCount = 0;
  var totalMissedInterestMonths = 0;
  var currentYearBe = getCurrentThaiYearBe_();

  for (var i = 0; i < myLoans.length; i++) {
    var loan = myLoans[i] || {};
    var balance = Number(loan.balance) || 0;
    if (balance > highestOutstandingValue) {
      highestOutstandingLoan = loan;
      highestOutstandingValue = balance;
    }
    var loanTxs = getIndexedRecordsByContract_(spreadsheet, loan.contract, true);
    var annualState = calculateLoanAnnualInterestState_(loan.createdAt, getPaidInstallmentsYtdFromTransactions_(loanTxs, currentYearBe), new Date(), loan.balance, loan.status);
    var missed = Math.max(0, Number((annualState || {}).missedInterestMonths) || 0);
    if (missed > 0) overdueLoanCount++;
    totalMissedInterestMonths += missed;
  }

  var latestTxAmountLabel = latestTx
    ? ('ต้น ' + addCommas(latestTx.principalPaid || 0) + ' | ดอก ' + addCommas(latestTx.interestPaid || 0) + ' | ' + String(latestTx.txStatus || '-'))
    : 'ยังไม่มีรายการรับชำระ';
  var latestNoteLabel = latestNote
    ? String(latestNote.noteText || '').trim().substring(0, 120)
    : 'ยังไม่มีหมายเหตุภายใน';
  var latestAttachmentLabel = latestAttachment
    ? (String(latestAttachment.fileName || '-').trim() + (latestAttachment.note ? ' | ' + String(latestAttachment.note).trim().substring(0, 80) : ''))
    : 'ยังไม่มีไฟล์แนบ';
  var highestOutstandingLabel = highestOutstandingLoan
    ? ('คงเหลือ ' + addCommas(highestOutstandingLoan.balance || 0) + ' | สถานะ ' + String(highestOutstandingLoan.status || '-'))
    : 'ยังไม่พบสัญญาของสมาชิก';

  return {
    title: 'สรุปเร็วสมาชิก',
    description: 'ดูจุดสำคัญล่าสุดเพื่อประเมินเคสก่อนลงมือทำรายการ',
    cards: [
      buildOverviewCard_('รับชำระล่าสุด', latestTx ? latestTx.timestamp : '-', latestTx ? ('สัญญา ' + String(latestTx.contract || '-') + ' | ' + latestTxAmountLabel) : latestTxAmountLabel, 'emerald', latestTx && latestTx.contract ? 'เปิดสัญญา' : '', 'contract', latestTx && latestTx.contract ? latestTx.contract : ''),
      buildOverviewCard_('สัญญาหนี้สูงสุด', highestOutstandingLoan ? String(highestOutstandingLoan.contract || '-') : '-', highestOutstandingLabel, 'rose', highestOutstandingLoan ? 'เปิดสัญญา' : '', 'contract', highestOutstandingLoan ? String(highestOutstandingLoan.contract || '') : ''),
      buildOverviewCard_('ค้างดอกรวม', addCommas(totalMissedInterestMonths) + ' งวด', overdueLoanCount > 0 ? ('ต้องติดตาม ' + addCommas(overdueLoanCount) + ' สัญญา') : 'ยังไม่พบสัญญาที่ค้างดอก', 'amber', memberId ? 'ค้นหาในคลังรายงาน' : '', 'report', memberId),
      buildOverviewCard_('หมายเหตุล่าสุด', latestNote ? (latestNote.updatedAt || latestNote.createdAt || '-') : '-', latestNoteLabel, 'cyan', '', '', ''),
      buildOverviewCard_('ไฟล์แนบล่าสุด', latestAttachment ? (latestAttachment.updatedAt || latestAttachment.createdAt || '-') : '-', latestAttachmentLabel, 'violet', latestAttachment && latestAttachment.fileUrl ? 'เปิดไฟล์แนบ' : '', latestAttachment && latestAttachment.fileUrl ? 'url' : '', latestAttachment && latestAttachment.fileUrl ? latestAttachment.fileUrl : '')
    ]
  };
}

function buildLoanQuickOverview_(result) {
  var detail = result || {};
  var loan = detail.loan || {};
  var borrower = detail.borrower || {};
  var notes = Array.isArray(detail.notes) ? detail.notes : [];
  var attachments = Array.isArray(detail.attachments) ? detail.attachments : [];
  var transactions = Array.isArray(detail.transactions) ? detail.transactions : [];
  var latestTx = pickLatestRecordByFields_(transactions, ['timestamp', 'dateOnly']);
  var latestNote = pickLatestRecordByFields_(notes, ['updatedAt', 'createdAt']);
  var latestAttachment = pickLatestRecordByFields_(attachments, ['updatedAt', 'createdAt']);
  var currentYearBe = getCurrentThaiYearBe_();
  var annualState = calculateLoanAnnualInterestState_(loan.createdAt, getPaidInstallmentsYtdFromTransactions_(transactions, currentYearBe), new Date(), loan.balance, loan.status);
  var missedInterestMonths = latestTx ? Math.max(0, Number(latestTx.missedAfterPayment) || 0) : Math.max(0, Number((annualState || {}).missedInterestMonths) || 0);
  var latestTxAmountLabel = latestTx
    ? ('ต้น ' + addCommas(latestTx.principalPaid || 0) + ' | ดอก ' + addCommas(latestTx.interestPaid || 0) + ' | คงเหลือ ' + addCommas(latestTx.newBalance || 0))
    : 'ยังไม่มีรายการรับชำระ';
  var latestNoteLabel = latestNote
    ? String(latestNote.noteText || '').trim().substring(0, 120)
    : 'ยังไม่มีหมายเหตุภายใน';
  var latestAttachmentLabel = latestAttachment
    ? (String(latestAttachment.fileName || '-').trim() + (latestAttachment.note ? ' | ' + String(latestAttachment.note).trim().substring(0, 80) : ''))
    : 'ยังไม่มีไฟล์แนบ';

  return {
    title: 'สรุปเร็วสัญญา',
    description: 'เห็นสถานะล่าสุดของสัญญาแบบรวดเร็ว ก่อนเปิดดูธุรกรรมเต็ม',
    cards: [
      buildOverviewCard_('รับชำระล่าสุด', latestTx ? latestTx.timestamp : '-', latestTx ? latestTxAmountLabel : latestTxAmountLabel, 'emerald', borrower && borrower.id ? 'เปิดสมาชิก' : '', 'member', borrower && borrower.id ? borrower.id : ''),
      buildOverviewCard_('ค้างดอกปัจจุบัน', addCommas(missedInterestMonths) + ' งวด', missedInterestMonths > 0 ? 'ควรติดตามการชำระดอกเบี้ย' : 'ไม่พบค้างดอก ณ ตอนนี้', 'amber', String(loan.contract || '').trim() ? 'ค้นหาในคลังรายงาน' : '', 'report', String(loan.contract || '').trim()),
      buildOverviewCard_('ผู้กู้', String(borrower.name || loan.member || '-').trim() || '-', 'รหัสสมาชิก ' + String(borrower.id || loan.memberId || '-').trim(), 'cyan', borrower && borrower.id ? 'เปิดสมาชิก' : '', 'member', borrower && borrower.id ? borrower.id : ''),
      buildOverviewCard_('หมายเหตุล่าสุด', latestNote ? (latestNote.updatedAt || latestNote.createdAt || '-') : '-', latestNoteLabel, 'violet', '', '', ''),
      buildOverviewCard_('ไฟล์แนบล่าสุด', latestAttachment ? (latestAttachment.updatedAt || latestAttachment.createdAt || '-') : '-', latestAttachmentLabel, 'rose', latestAttachment && latestAttachment.fileUrl ? 'เปิดไฟล์แนบ' : '', latestAttachment && latestAttachment.fileUrl ? 'url' : '', latestAttachment && latestAttachment.fileUrl ? latestAttachment.fileUrl : '')
    ]
  };
}

var __phase19_orig_getMemberDetail = getMemberDetail;
getMemberDetail = function(memberId) {
  var result = __phase19_orig_getMemberDetail(memberId);
  if (!result || result.status === 'Error') return result;
  try {
    var ss = getDatabaseSpreadsheet_();
    result.quickOverview = buildMemberQuickOverview_(ss, result);
  } catch (e) {
    result.quickOverview = { title: 'สรุปเร็วสมาชิก', description: 'ไม่สามารถคำนวณสรุปเร็วได้', cards: [] };
  }
  return result;
};

var __phase19_orig_getLoanDetail = getLoanDetail;
getLoanDetail = function(contractNo) {
  var result = __phase19_orig_getLoanDetail(contractNo);
  if (!result || result.status === 'Error') return result;
  try {
    result.quickOverview = buildLoanQuickOverview_(result);
  } catch (e) {
    result.quickOverview = { title: 'สรุปเร็วสัญญา', description: 'ไม่สามารถคำนวณสรุปเร็วได้', cards: [] };
  }
  return result;
};
