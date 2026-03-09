const menuToggle = document.getElementById('menuToggle');
const mobileMenu = document.getElementById('mobileMenu');
const year = document.getElementById('year');
const contactForm = document.getElementById('contactForm');

if (year) year.textContent = new Date().getFullYear();

if (menuToggle && mobileMenu) {
  menuToggle.addEventListener('click', () => {
    mobileMenu.classList.toggle('open');
  });

  mobileMenu.querySelectorAll('a').forEach(link => {
    link.addEventListener('click', () => mobileMenu.classList.remove('open'));
  });
}

if (contactForm) {
  contactForm.addEventListener('submit', (event) => {
    event.preventDefault();

    const name = document.getElementById('name').value.trim();
    const email = document.getElementById('email').value.trim();
    const company = document.getElementById('company').value.trim();
    const message = document.getElementById('message').value.trim();

    const subject = encodeURIComponent(`Brick Brick inquiry from ${name || 'new lead'}`);
    const body = encodeURIComponent(
`Name: ${name}
Email: ${email}
Company: ${company}

Project details:
${message}`
    );

    window.location.href = `mailto:contact@brick-brick.org?subject=${subject}&body=${body}`;
  });
}
