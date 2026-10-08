#!/bin/sh
git filter-branch -f --env-filter '
CORRECT_NAME="Asylbek Abdibay"
CORRECT_EMAIL="asylbekabdibay@gmail.com"

if [ "$GIT_COMMITTER_EMAIL" = "aidos@example.com" ] || [ "$GIT_COMMITTER_EMAIL" = "7469966@gmail.com" ] || [ "$GIT_COMMITTER_EMAIL" = "ci@argusai.kz" ] || [ "$GIT_COMMITTER_EMAIL" = "deploy@argusai.kz" ] || [ "$GIT_COMMITTER_EMAIL" = "myrzataevddd0606@gmail.com" ]; then
    export GIT_COMMITTER_NAME="$CORRECT_NAME"
    export GIT_COMMITTER_EMAIL="$CORRECT_EMAIL"
fi
if [ "$GIT_AUTHOR_EMAIL" = "aidos@example.com" ] || [ "$GIT_AUTHOR_EMAIL" = "7469966@gmail.com" ] || [ "$GIT_AUTHOR_EMAIL" = "ci@argusai.kz" ] || [ "$GIT_AUTHOR_EMAIL" = "deploy@argusai.kz" ] || [ "$GIT_AUTHOR_EMAIL" = "myrzataevddd0606@gmail.com" ]; then
    export GIT_AUTHOR_NAME="$CORRECT_NAME"
    export GIT_AUTHOR_EMAIL="$CORRECT_EMAIL"
fi
' --tag-name-filter cat -- --branches --tags
