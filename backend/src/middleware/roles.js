function normalizeRole(role) {
  return String(role || "").trim().toLowerCase();
}

function requireRoles(...allowedRoles) {
  const allowed = allowedRoles.map(normalizeRole);
  return (req, res, next) => {
    const userRole = normalizeRole(req.user?.role);
    if (!userRole || !allowed.includes(userRole)) {
      return res.status(403).json({ message: "Sem permissao para este recurso." });
    }
    return next();
  };
}

module.exports = { requireRoles, normalizeRole };
