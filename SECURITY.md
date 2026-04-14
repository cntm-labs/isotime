# Security Policy

## 🛡️ Commitment
We take the security of `isotime` seriously. This project aims for a **High (Mission Critical)** security standard.

## 📢 Reporting a Vulnerability
Please do not report security vulnerabilities through public GitHub issues. Instead, send a detailed report to:
**security@cntm-labs.com**

### What to include:
- A description of the vulnerability.
- Steps to reproduce (PoC).
- Potential impact.

## 🔐 Security Protocols
All storage protocols must be formally verified using Lean. Zero-trust encryption (E2EE) is mandatory for all data at rest.

- **Dependency Management:** Regularly scan for vulnerable packages.
- **CI/CD Security:** Mandatory automated security scans are integrated into `.github/workflows/security.yml`.
- **Disclosure:** We follow a responsible disclosure timeline.
